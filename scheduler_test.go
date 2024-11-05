package transaction_outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func newID() uuid.UUID {
	id, _ := uuid.NewV7()

	return id
}

func TestScheduler_Add(t *testing.T) {
	errCh := make(chan error)
	store := NewMemoryStore()
	store.SetErrorTaskCnt(1)
	store.SetDoneTaskCnt(1)

	scheduler, _ := NewScheduler(context.Background(), store, 1, time.Second, errCh)

	assert.NoError(t, scheduler.Add(Task{
		Code:      "t1",
		Status:    ToDo,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
	}))

	toDoTasks, _ := store.ToDoTasks()
	assert.Len(t, toDoTasks, 1)
	assert.Equal(t, TaskCode("t1"), toDoTasks[0].Code)
	assert.Equal(t, ToDo, toDoTasks[0].Status)
}

func TestScheduler_Register(t *testing.T) {
	errCh := make(chan error)
	store := NewMemoryStore()
	store.SetErrorTaskCnt(1)
	store.SetDoneTaskCnt(1)

	scheduler, _ := NewScheduler(context.Background(), store, 1, time.Second, errCh)

	assert.NoError(t, scheduler.Register(&DefaultHandler{
		TaskCode: "t1",
	}))

	assert.ErrorIs(t, scheduler.Register(&DefaultHandler{
		TaskCode: "t1",
	}), ErrHandlerAlreadyExists)

	assert.NoError(t, scheduler.Register(&DefaultHandler{
		TaskCode: "t2",
	}))
}

func TestScheduler_handleRetry(t *testing.T) {
	errCh := make(chan error)
	store := NewMemoryStore()
	store.SetErrorTaskCnt(1)
	store.SetDoneTaskCnt(1)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	scheduler, _ := NewScheduler(ctx, store, 1, time.Millisecond, errCh)

	runOnSuccess, runOnError := false, false

	hErr := errors.New("handle err")

	assert.NoError(t, scheduler.Register(&DefaultHandler{
		TaskCode: TaskCode("t1"),
		HandleFn: func(data []byte) error {
			return hErr
		},
		OnSuccessFn: func(data []byte) {
			runOnSuccess = true
		},
		OnErrorFn: func(data []byte) {
			runOnError = true
		},
		RetryTimeout:    time.Hour,
		MaxRetryTimeCnt: 3,
	}))

	testTask := Task{
		Id:        newID(),
		Code:      TaskCode("t1"),
		Status:    ToDo,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  0,
		Data:      nil,
		Errors:    nil,
	}

	scheduler.taskCh <- testTask

	scheduler.wg.Add(1)
	go scheduler.handle()
	close(scheduler.taskCh)
	scheduler.Wait()

	assert.False(t, runOnSuccess)
	assert.True(t, runOnError)

	todoTasks, _ := store.ToDoTasks()

	assert.Equal(t, testTask.Id, todoTasks[0].Id)
	assert.Equal(t, testTask.Code, todoTasks[0].Code)
	assert.Equal(t, ToDo, todoTasks[0].Status)
	assert.Equal(t, testTask.Data, todoTasks[0].Data)
	assert.Equal(t, map[uint8]error{uint8(1): hErr}, todoTasks[0].Errors)
	assert.Equal(t, testTask.RetryCnt+1, todoTasks[0].RetryCnt)
	assert.Equal(t, testTask.CreatedAt, todoTasks[0].CreatedAt)
	assert.True(t, time.Now().Before(todoTasks[0].RetryOn))
}

func TestScheduler_handleDone(t *testing.T) {
	errCh := make(chan error)
	store := NewMemoryStore()
	store.SetErrorTaskCnt(1)
	store.SetDoneTaskCnt(1)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	scheduler, _ := NewScheduler(ctx, store, 1, time.Millisecond, errCh)

	runOnSuccess, runOnError := false, false

	assert.NoError(t, scheduler.Register(&DefaultHandler{
		TaskCode: TaskCode("t1"),
		HandleFn: func(data []byte) error {
			return nil
		},
		OnSuccessFn: func(data []byte) {
			runOnSuccess = true
		},
		OnErrorFn: func(data []byte) {
			runOnError = true
		},
		RetryTimeout:    0,
		MaxRetryTimeCnt: 0,
	}))

	testTask := Task{
		Id:        newID(),
		Code:      TaskCode("t1"),
		Status:    ToDo,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  0,
		Data:      nil,
		Errors:    nil,
	}

	scheduler.taskCh <- testTask

	scheduler.wg.Add(1)
	go scheduler.handle()
	close(scheduler.taskCh)
	scheduler.Wait()

	assert.True(t, runOnSuccess)
	assert.False(t, runOnError)

	doneTasks, _ := store.DoneTasks()

	assert.Equal(t, testTask.Id, doneTasks[0].Id)
	assert.Equal(t, testTask.Code, doneTasks[0].Code)
	assert.Equal(t, Done, doneTasks[0].Status)
	assert.Equal(t, testTask.Data, doneTasks[0].Data)
	assert.Equal(t, testTask.Errors, doneTasks[0].Errors)
	assert.Equal(t, testTask.RetryCnt, doneTasks[0].RetryCnt)
	assert.Equal(t, testTask.CreatedAt, doneTasks[0].CreatedAt)
	assert.Equal(t, testTask.RetryOn, doneTasks[0].RetryOn)
}

func TestScheduler_handleError(t *testing.T) {
	errCh := make(chan error)
	store := NewMemoryStore()
	store.SetErrorTaskCnt(1)
	store.SetDoneTaskCnt(1)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	scheduler, _ := NewScheduler(ctx, store, 1, time.Millisecond, errCh)

	runOnSuccess, runOnError := false, false

	hErr := errors.New("handle err")

	assert.NoError(t, scheduler.Register(&DefaultHandler{
		TaskCode: TaskCode("t1"),
		HandleFn: func(data []byte) error {
			return hErr
		},
		OnSuccessFn: func(data []byte) {
			runOnSuccess = true
		},
		OnErrorFn: func(data []byte) {
			runOnError = true
		},
		RetryTimeout:    time.Hour,
		MaxRetryTimeCnt: 3,
	}))

	testTask := Task{
		Id:        newID(),
		Code:      TaskCode("t1"),
		Status:    ToDo,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  2,
		Data:      nil,
		Errors:    nil,
	}

	scheduler.taskCh <- testTask

	scheduler.wg.Add(1)
	go scheduler.handle()
	close(scheduler.taskCh)
	scheduler.Wait()

	assert.False(t, runOnSuccess)
	assert.True(t, runOnError)

	errorTasks, _ := store.ErrorTasks()

	assert.Equal(t, testTask.Id, errorTasks[0].Id)
	assert.Equal(t, testTask.Code, errorTasks[0].Code)
	assert.Equal(t, Error, errorTasks[0].Status)
	assert.Equal(t, testTask.Data, errorTasks[0].Data)
	assert.Equal(t, map[uint8]error{uint8(3): hErr}, errorTasks[0].Errors)
	assert.Equal(t, testTask.RetryCnt+1, errorTasks[0].RetryCnt)
	assert.Equal(t, testTask.CreatedAt, errorTasks[0].CreatedAt)
	assert.Equal(t, testTask.RetryOn, errorTasks[0].RetryOn)

}

func TestScheduler_extract(t *testing.T) {
	errCh := make(chan error)
	store := NewMemoryStore()
	store.SetErrorTaskCnt(1)
	store.SetDoneTaskCnt(1)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	scheduler, _ := NewScheduler(ctx, store, 1, time.Millisecond, errCh)

	go scheduler.extract()

	task := Task{
		Id:        newID(),
		Code:      "t1",
		Status:    ToDo,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  0,
		Data:      nil,
		Errors:    nil,
	}

	assert.NoError(t, scheduler.Add(task))

	outTask := <-scheduler.taskCh

	assert.Equal(t, task.Id, outTask.Id)
	assert.Equal(t, task.Code, outTask.Code)
	assert.Equal(t, task.Status, outTask.Status)
}
