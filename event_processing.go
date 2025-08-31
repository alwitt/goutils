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
		StartEventLoop starts one daemon thread for processing the submitted task-parameters

		 @param wg *sync.WaitGroup - wait group
		 @return whether successful
	*/
	StartEventLoop(wg *sync.WaitGroup) error

	/*
		StopEventLoop stops the daemon threads

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
	lock             sync.RWMutex
}

/*
GetNewTaskProcessorInstance get TaskProcessor

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
		lock:             sync.RWMutex{},
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
	p.lock.Lock()
	defer p.lock.Unlock()
	p.executionMap = newMap
	return nil
}

/*
AddToTaskExecutionMap add new (task-parameter, handler function) mapping to the existing set.

	@param parameterType reflect.Type - task-parameter object type
	@param handler TaskHandler - task handler
	@return whether successful
*/
func (p *taskProcessorImpl) AddToTaskExecutionMap(
	theType reflect.Type, handler TaskProcessorSupportHandler,
) error {
	log.WithFields(p.LogTags).Debugf("Appending to task execution mapping for %s", theType)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.executionMap[theType] = handler
	return nil
}

// getHandler
func (p *taskProcessorImpl) getHandler(newTaskParam interface{}) (
	TaskProcessorSupportHandler, bool,
) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if len(p.executionMap) < 1 {
		return nil, false
	}
	handler, ok := p.executionMap[reflect.TypeOf(newTaskParam)]
	return handler, ok
}

// processNewTaskParam process a new task parameter based on use defined mapping
func (p *taskProcessorImpl) processNewTaskParam(newTaskParam interface{}) error {
	defer func() {
		if p.metrics != nil {
			p.metrics.RecordProcessed(p.name)
		}
	}()

	// Process task based on the parameter type
	if theHandler, ok := p.getHandler(newTaskParam); ok {
		return theHandler(newTaskParam)
	}
	return fmt.Errorf(
		"[TP %s] No matching handler found for %s", p.name, reflect.TypeOf(newTaskParam),
	)
}

/*
StartEventLoop starts one daemon thread for processing the submitted task-parameters

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
StopEventLoop stops the daemon threads

	@return whether successful
*/
func (p *taskProcessorImpl) StopEventLoop() error {
	p.contextCancel()
	return nil
}
