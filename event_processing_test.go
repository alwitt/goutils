package goutils

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
)

func TestTaskParamProcessing(t *testing.T) {
	assert := assert.New(t)

	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()
	uut, err := GetNewTaskProcessorInstance(
		ctxt, "testing", 4, log.Fields{"instance": "unit-tester"}, nil,
	)
	assert.Nil(err)
	defer func() {
		assert.Nil(uut.StopEventLoop())
	}()
	uutCast, ok := uut.(*taskProcessorImpl)
	assert.True(ok)

	// Case 1: no executor map
	{
		assert.NotNil(uutCast.processNewTaskParam("hello"))
	}

	type testStruct1 struct{}
	type testStruct2 struct{}
	type testStruct3 struct{}

	executorMap := map[reflect.Type]TaskProcessorSupportHandler{
		reflect.TypeOf(testStruct1{}): func(_ interface{}) error {
			return nil
		},
	}

	// Case 2: define a executor map
	{
		assert.Nil(uut.SetTaskExecutionMap(executorMap))
		assert.Nil(uutCast.processNewTaskParam(testStruct1{}))
		assert.NotNil(uutCast.processNewTaskParam(testStruct2{}))
		assert.NotNil(uutCast.processNewTaskParam(&testStruct3{}))
	}

	executorMap = map[reflect.Type]TaskProcessorSupportHandler{
		reflect.TypeOf(testStruct1{}): func(_ interface{}) error { return nil },
		reflect.TypeOf(testStruct3{}): func(_ interface{}) error { return fmt.Errorf("Dummy error") },
	}

	// Case 3: change executor map
	{
		assert.Nil(uut.SetTaskExecutionMap(executorMap))
		assert.Nil(uutCast.processNewTaskParam(testStruct1{}))
		assert.NotNil(uutCast.processNewTaskParam(&testStruct2{}))
		assert.NotNil(uutCast.processNewTaskParam(testStruct3{}))
	}

	// Case 4: append to existing map
	{
		assert.Nil(uut.AddToTaskExecutionMap(
			reflect.TypeOf(&testStruct2{}), func(_ interface{}) error { return nil },
		))
		assert.Nil(uutCast.processNewTaskParam(testStruct1{}))
		assert.Nil(uutCast.processNewTaskParam(&testStruct2{}))
		assert.NotNil(uutCast.processNewTaskParam(testStruct3{}))
	}
}

func TestTaskDemuxProcessing(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()

	uut, err := GetNewTaskProcessorInstance(
		ctxt, "testing", 3, log.Fields{"instance": "unit-tester"}, nil,
	)
	assert.Nil(err)
	defer func() {
		assert.Nil(uut.StopEventLoop())
	}()

	// start the built in processes
	for itr := 0; itr < 3; itr++ {
		assert.Nil(uut.StartEventLoop(&wg))
	}

	path1 := 0
	path2 := 0
	path3 := 0

	type testStruct1 struct{}
	type testStruct2 struct{}
	type testStruct3 struct{}

	testWG := sync.WaitGroup{}
	pathCB1 := func(_ interface{}) error {
		path1++
		testWG.Done()
		return nil
	}
	pathCB2 := func(_ interface{}) error {
		path2++
		testWG.Done()
		return nil
	}
	pathCB3 := func(_ interface{}) error {
		path3++
		testWG.Done()
		return nil
	}

	executorMap := map[reflect.Type]TaskProcessorSupportHandler{
		reflect.TypeOf(testStruct1{}): pathCB1,
		reflect.TypeOf(testStruct2{}): pathCB2,
		reflect.TypeOf(testStruct3{}): pathCB3,
	}

	assert.Nil(uut.SetTaskExecutionMap(executorMap))

	// Case 1: trigger
	{
		testWG.Add(1)
		useContext, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Submit(useContext, testStruct1{}))
		cancel()
		testWG.Wait()
		assert.Equal(1, path1)
	}

	// Case 2: trigger
	{
		testWG.Add(1)
		useContext, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Submit(useContext, testStruct1{}))
		cancel()
		testWG.Wait()
		assert.Equal(2, path1)
	}

	// Case 3: trigger back to back
	{
		testWG.Add(2)
		useContext, cancel := context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Submit(useContext, testStruct2{}))
		cancel()
		useContext, cancel = context.WithTimeout(context.Background(), time.Second)
		assert.Nil(uut.Submit(useContext, testStruct3{}))
		cancel()
		testWG.Wait()
		assert.Equal(1, path2)
		assert.Equal(1, path3)
	}
}
