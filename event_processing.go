package goutils

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/apex/log"
)

// TaskProcessorSupportHandler is the function signature of callback used to process an user task
type TaskProcessorSupportHandler func(taskParam interface{}) error

// TaskProcessor implements an event loop where tasks are processed by a daemon thread
type TaskProcessor interface {
	/*
		Submit submits a new task parameter to be processed by a handler

		 @param ctx context.Context - calling context
		 @param newTaskParam interface{} - task-parameter
		 @return whether successful
	*/
	Submit(ctx context.Context, newTaskParam interface{}) error

	/*
		SetTaskExecutionMap update the mapping between task-parameter object and its associated
		handler function.

		The task-parameter object contains information need to execute a particular task. When
		a user wants to execute a task, the user is submitting a task-parameter object via Submit.
		The module finds the associated handler function and calls it with the task-parameter object.

		 @param newMap map[reflect.Type]TaskHandler - map of task handlers to various task-parameter
		 object types
		 @return whether successful
	*/
	SetTaskExecutionMap(newMap map[reflect.Type]TaskProcessorSupportHandler) error

	/*
		AddToTaskExecutionMap add new (task-parameter, handler function) mapping to the existing set.

		 @param parameterType reflect.Type - task-parameter object type
		 @param handler TaskHandler - task handler
		 @return whether successful
	*/
	AddToTaskExecutionMap(parameterType reflect.Type, handler TaskProcessorSupportHandler) error

	/*
		StartEventLoop starts the daemon thread for processing the submitted task-parameters

		 @param wg *sync.WaitGroup - wait group
		 @return whether successful
	*/
	StartEventLoop(wg *sync.WaitGroup) error

	/*
		StopEventLoop stops the daemon thread

		 @return whether successful
	*/
	StopEventLoop() error
}

// taskProcessorImpl implements TaskProcessor which uses only one daemon thread
type taskProcessorImpl struct {
	Component
	name             string
	operationContext context.Context
	contextCancel    context.CancelFunc
	newTasks         chan interface{}
	executionMap     map[reflect.Type]TaskProcessorSupportHandler
	metrics          TaskProcessorMetricHelper
}

/*
GetNewTaskProcessorInstance get single threaded implementation of TaskProcessor

	@param ctxt context.Context - parent context
	@param instanceName string - instance name
	@param taskBufferLen int - number of task-parameters to buffer
	@param logTags log.Fields - metadata fields to include in the logs
	@param metricsHelper TaskProcessorMetricHelper - metrics collections helper
	@return new TaskProcessor instance
*/
func GetNewTaskProcessorInstance(
	ctxt context.Context,
	instanceName string,
	taskBufferLen int,
	logTags log.Fields,
	metricsHelper TaskProcessorMetricHelper,
) (TaskProcessor, error) {
	optCtxt, cancel := context.WithCancel(ctxt)
	return &taskProcessorImpl{
		Component:        Component{LogTags: logTags},
		name:             instanceName,
		operationContext: optCtxt,
		contextCancel:    cancel,
		newTasks:         make(chan interface{}, taskBufferLen),
		executionMap:     make(map[reflect.Type]TaskProcessorSupportHandler),
		metrics:          metricsHelper,
	}, nil
}

/*
Submit submits a new task parameter to be processed by a handler

	@param ctx context.Context - calling context
	@param newTaskParam interface{} - task-parameter
	@return whether successful
*/
func (p *taskProcessorImpl) Submit(ctx context.Context, newTaskParam interface{}) error {
	select {
	case p.newTasks <- newTaskParam:
		if p.metrics != nil {
			p.metrics.RecordSubmit(p.name, true)
		}
		return nil
	case <-ctx.Done():
		if p.metrics != nil {
			p.metrics.RecordSubmit(p.name, false)
		}
		return ctx.Err()
	case <-p.operationContext.Done():
		return p.operationContext.Err()
	}
}

/*
SetTaskExecutionMap update the mapping between task-parameter object and its associated
handler function.

The task-parameter object contains information need to execute a particular task. When
a user wants to execute a task, the user is submitting a task-parameter object via Submit.
The module finds the associated handler function and calls it with the task-parameter object.

	@param newMap map[reflect.Type]TaskHandler - map of task handlers to various task-parameter
	object types
	@return whether successful
*/
func (p *taskProcessorImpl) SetTaskExecutionMap(
	newMap map[reflect.Type]TaskProcessorSupportHandler,
) error {
	log.WithFields(p.LogTags).Debug("Changing task execution mapping")
	p.executionMap = newMap
	return nil
}

/*
AddToTaskExecutionMap add new (task-parameter, handler function) mapping to the existing set.

	@param parameterType reflect.Type - task-parameter object type
	@param handler TaskHandler - task handler
	@return whether successful
*/
func (p *taskProcessorImpl) AddToTaskExecutionMap(theType reflect.Type, handler TaskProcessorSupportHandler) error {
	log.WithFields(p.LogTags).Debugf("Appending to task execution mapping for %s", theType)
	p.executionMap[theType] = handler
	return nil
}

// StartEventLoop starts the daemon thread for processing the submitted task-parameters
func (p *taskProcessorImpl) processNewTaskParam(newTaskParam interface{}) error {
	defer func() {
		if p.metrics != nil {
			p.metrics.RecordProcessed(p.name)
		}
	}()
	if len(p.executionMap) > 0 {
		log.WithFields(p.LogTags).Debugf("Processing new %s", reflect.TypeOf(newTaskParam))
		// Process task based on the parameter type
		if theHandler, ok := p.executionMap[reflect.TypeOf(newTaskParam)]; ok {
			return theHandler(newTaskParam)
		}
		return fmt.Errorf(
			"[TP %s] No matching handler found for %s", p.name, reflect.TypeOf(newTaskParam),
		)
	}
	return fmt.Errorf("[TP %s] No task execution mapping set", p.name)
}

/*
StartEventLoop starts the daemon thread for processing the submitted task-parameters

	@param wg *sync.WaitGroup - wait group
	@return whether successful
*/
func (p *taskProcessorImpl) StartEventLoop(wg *sync.WaitGroup) error {
	log.WithFields(p.LogTags).Info("Starting event loop")
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.WithFields(p.LogTags).Info("Event loop exited")
		finished := false
		for !finished {
			select {
			case <-p.operationContext.Done():
				finished = true
			case newTaskParam, ok := <-p.newTasks:
				if !ok {
					log.WithFields(p.LogTags).Error(
						"Event loop terminating. Failed to read new task param",
					)
					return
				}
				if err := p.processNewTaskParam(newTaskParam); err != nil {
					log.WithError(err).WithFields(p.LogTags).Error("Failed to process new task param")
				}
			}
		}
	}()
	return nil
}

/*
StopEventLoop stops the daemon thread

	@return whether successful
*/
func (p *taskProcessorImpl) StopEventLoop() error {
	p.contextCancel()
	return nil
}

// ==============================================================================

// taskDemuxProcessorImpl implement TaskProcessor but support multiple parallel workers
type taskDemuxProcessorImpl struct {
	Component
	name             string
	input            TaskProcessor
	workers          []TaskProcessor
	routeIdx         int
	operationContext context.Context
	contextCancel    context.CancelFunc
}

/*
GetNewTaskDemuxProcessorInstance get multi-threaded implementation of TaskProcessor

	@param ctxt context.Context - parent context
	@param instanceName string - instance name
	@param taskBufferLen int - number of task-parameters to buffer
	@param workerNum int - number of supporting worker threads
	@param logTags log.Fields - metadata fields to include in the logs
	@param metricsHelper TaskProcessorMetricHelper - metrics collections helper
	@return new TaskProcessor instance
*/
func GetNewTaskDemuxProcessorInstance(
	ctxt context.Context,
	instanceName string,
	taskBufferLen int,
	workerNum int,
	logTags log.Fields,
	metricsHelper TaskProcessorMetricHelper,
) (TaskProcessor, error) {
	if workerNum < 2 {
		return nil, fmt.Errorf("won't create multi-threaded TaskProcessor with less than two workers")
	}
	optCtxt, cancel := context.WithCancel(ctxt)
	inputTP, err := GetNewTaskProcessorInstance(
		optCtxt, fmt.Sprintf("%s.input", instanceName), taskBufferLen, logTags, metricsHelper,
	)
	if err != nil {
		cancel()
		return nil, err
	}
	workers := make([]TaskProcessor, workerNum)
	for itr := 0; itr < workerNum; itr++ {
		perWorkerLogTags := log.Fields{}
		for field, value := range logTags {
			perWorkerLogTags[field] = value
		}
		perWorkerLogTags["worker"] = itr
		workerTP, err := GetNewTaskProcessorInstance(
			optCtxt,
			fmt.Sprintf("%s.worker.%d", instanceName, itr),
			taskBufferLen,
			perWorkerLogTags,
			metricsHelper,
		)
		if err != nil {
			cancel()
			return nil, err
		}
		workers[itr] = workerTP
	}
	return &taskDemuxProcessorImpl{
		name:             instanceName,
		input:            inputTP,
		workers:          workers,
		routeIdx:         0,
		operationContext: optCtxt,
		contextCancel:    cancel,
		Component:        Component{LogTags: logTags},
	}, nil
}

/*
Submit submits a new task parameter to be processed by a handler

	@param ctx context.Context - calling context
	@param newTaskParam interface{} - task-parameter
	@return whether successful
*/
func (p *taskDemuxProcessorImpl) Submit(ctx context.Context, newTaskParam interface{}) error {
	return p.input.Submit(ctx, newTaskParam)
}

// processNewTaskParam execute a submitted task-parameter
func (p *taskDemuxProcessorImpl) processNewTaskParam(newTaskParam interface{}) error {
	if len(p.workers) > 0 {
		log.WithFields(p.LogTags).Debugf("Processing new %s", reflect.TypeOf(newTaskParam))
		defer func() { p.routeIdx = (p.routeIdx + 1) % len(p.workers) }()
		return p.workers[p.routeIdx].Submit(p.operationContext, newTaskParam)
	}
	return fmt.Errorf("[TDP %s] No workers defined", p.name)
}

/*
SetTaskExecutionMap update the mapping between task-parameter object and its associated
handler function.

The task-parameter object contains information need to execute a particular task. When
a user wants to execute a task, the user is submitting a task-parameter object via Submit.
The module finds the associated handler function and calls it with the task-parameter object.

	@param newMap map[reflect.Type]TaskHandler - map of task handlers to various task-parameter
	object types
	@return whether successful
*/
func (p *taskDemuxProcessorImpl) SetTaskExecutionMap(newMap map[reflect.Type]TaskProcessorSupportHandler) error {
	for _, worker := range p.workers {
		_ = worker.SetTaskExecutionMap(newMap)
	}
	// Create a different version of the input to route to worker
	inputMap := map[reflect.Type]TaskProcessorSupportHandler{}
	for msgType := range newMap {
		inputMap[msgType] = p.processNewTaskParam
	}
	return p.input.SetTaskExecutionMap(inputMap)
}

/*
AddToTaskExecutionMap add new (task-parameter, handler function) mapping to the existing set.

	@param parameterType reflect.Type - task-parameter object type
	@param handler TaskHandler - task handler
	@return whether successful
*/
func (p *taskDemuxProcessorImpl) AddToTaskExecutionMap(
	theType reflect.Type, handler TaskProcessorSupportHandler,
) error {
	for _, worker := range p.workers {
		_ = worker.AddToTaskExecutionMap(theType, handler)
	}
	// Do the same for input
	return p.input.AddToTaskExecutionMap(theType, p.processNewTaskParam)
}

/*
StartEventLoop starts the daemon thread for processing the submitted task-parameters

	@param wg *sync.WaitGroup - wait group
	@return whether successful
*/
func (p *taskDemuxProcessorImpl) StartEventLoop(wg *sync.WaitGroup) error {
	log.WithFields(p.LogTags).Info("Starting event loops")
	// Start the worker loops first
	for _, worker := range p.workers {
		_ = worker.StartEventLoop(wg)
	}
	// Start the input loop
	return p.input.StartEventLoop(wg)
}

/*
StopEventLoop stops the daemon thread

	@return whether successful
*/
func (p *taskDemuxProcessorImpl) StopEventLoop() error {
	p.contextCancel()
	return nil
}
