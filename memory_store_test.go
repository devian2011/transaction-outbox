package transaction_outbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMemoryStore(t *testing.T) {
	store := NewMemoryStore()

	assert.Equal(t, defaultErrorTasksCnt, store.errorTaskCnt)
	assert.Equal(t, defaultDoneTasksCnt, store.doneTaskCnt)

	store.SetErrorTaskCnt(uint32(1))
	store.SetDoneTaskCnt(uint32(1))

	assert.Equal(t, uint32(1), store.errorTaskCnt)
	assert.Equal(t, uint32(1), store.doneTaskCnt)

	toDoTaskId := newID()
	assert.NoError(t, store.Save(Task{
		Id:        toDoTaskId,
		Code:      "t1",
		Status:    ToDo,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  0,
		Data:      nil,
		Errors:    nil,
	}))

	assert.NoError(t, store.Save(Task{
		Id:        newID(),
		Code:      "t2",
		Status:    Done,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  0,
		Data:      nil,
		Errors:    nil,
	}))

	assert.NoError(t, store.Save(Task{
		Id:        newID(),
		Code:      "t3",
		Status:    Error,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  0,
		Data:      nil,
		Errors:    nil,
	}))

	assert.ErrorIs(t, store.Save(Task{
		Id:        newID(),
		Code:      "t3",
		Status:    TaskStatus(10),
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  0,
		Data:      nil,
		Errors:    nil,
	}), ErrUnknownTaskStatus)

	toDoTasks, _ := store.ToDoTasks()
	assert.Len(t, toDoTasks, 1)
	assert.Equal(t, ToDo, toDoTasks[0].Status)
	assert.Equal(t, TaskCode("t1"), toDoTasks[0].Code)

	errTasks, _ := store.ErrorTasks()
	assert.Len(t, errTasks, 1)
	assert.Equal(t, Error, errTasks[0].Status)
	assert.Equal(t, TaskCode("t3"), errTasks[0].Code)

	doneTasks, _ := store.DoneTasks()
	assert.Len(t, doneTasks, 1)
	assert.Equal(t, Done, doneTasks[0].Status)
	assert.Equal(t, TaskCode("t2"), doneTasks[0].Code)

	assert.NoError(t, store.Save(Task{
		Id:        toDoTaskId,
		Code:      "t4",
		Status:    Done,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
		RetryOn:   time.Time{},
		RetryCnt:  0,
		Data:      nil,
		Errors:    nil,
	}))

	toDoTasks, _ = store.DoneTasks()
	assert.Len(t, toDoTasks, 1)
	assert.Equal(t, Done, toDoTasks[0].Status)
	assert.Equal(t, TaskCode("t4"), toDoTasks[0].Code)

	assert.Len(t, store.todoTasks, 0)
}
