package transaction_outbox

import (
	"sync"

	"github.com/google/uuid"
)

const (
	defaultDoneTasksCnt  = uint32(100)
	defaultErrorTasksCnt = uint32(100)
)

type MemoryStore struct {
	history   map[TaskStatus][]Task
	todoTasks map[uuid.UUID]Task

	doneTaskCnt  uint32
	errorTaskCnt uint32

	mtx        *sync.RWMutex
	mtxHistory *sync.RWMutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		history:      make(map[TaskStatus][]Task),
		todoTasks:    make(map[uuid.UUID]Task),
		mtxHistory:   &sync.RWMutex{},
		mtx:          &sync.RWMutex{},
		doneTaskCnt:  defaultDoneTasksCnt,
		errorTaskCnt: defaultErrorTasksCnt,
	}
}

func (m *MemoryStore) SetDoneTaskCnt(cnt uint32) {
	m.doneTaskCnt = cnt
}

func (m *MemoryStore) SetErrorTaskCnt(cnt uint32) {
	m.errorTaskCnt = cnt
}

func (m *MemoryStore) saveToDo(task Task) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.todoTasks[task.Id] = task

	return nil
}

func (m *MemoryStore) saveHistory(task Task) error {
	m.mtxHistory.Lock()
	defer m.mtxHistory.Unlock()

	if _, exists := m.history[task.Status]; exists {
		switch task.Status {
		case Error:
			if uint32(len(m.history[task.Status])) >= m.errorTaskCnt {
				m.history[task.Status] = make([]Task, 0, m.errorTaskCnt)
			}
		case Done:
			if uint32(len(m.history[task.Status])) >= m.doneTaskCnt {
				m.history[task.Status] = make([]Task, 0, m.doneTaskCnt)
			}
		default:
			return ErrUnknownTaskStatus
		}
	} else {
		switch task.Status {
		case Error:
			m.history[task.Status] = make([]Task, 0, m.errorTaskCnt)
		case Done:
			m.history[task.Status] = make([]Task, 0, m.doneTaskCnt)
		default:
			return ErrUnknownTaskStatus
		}
	}

	m.history[task.Status] = append(m.history[task.Status], task)

	m.mtx.Lock()
	delete(m.todoTasks, task.Id)
	m.mtx.Unlock()

	return nil
}

func (m *MemoryStore) Save(task Task) error {
	if task.Status == ToDo {
		return m.saveToDo(task)
	} else {
		return m.saveHistory(task)
	}
}

func (m *MemoryStore) getTaskByStatus(status TaskStatus) ([]Task, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	if h, exists := m.history[status]; exists {
		return h, nil
	} else {
		return []Task{}, nil
	}
}

func (m *MemoryStore) ToDoTasks() ([]Task, error) {
	result := make([]Task, 0, len(m.todoTasks))
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	for i := range m.todoTasks {
		result = append(result, m.todoTasks[i])
	}

	return result, nil
}

func (m *MemoryStore) ErrorTasks() ([]Task, error) {
	return m.getTaskByStatus(Error)
}

func (m *MemoryStore) DoneTasks() ([]Task, error) {
	return m.getTaskByStatus(Done)
}
