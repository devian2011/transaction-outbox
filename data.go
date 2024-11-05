package transaction_outbox

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrHandleFunctionIsNotSet = errors.New("handle function is not set")

	ErrUnknownTaskStatus = errors.New("error unknown task status")
)

type Store interface {
	ToDoTasks() ([]Task, error)
	Save(task Task) error
}

type DefaultHandler struct {
	TaskCode        TaskCode
	HandleFn        HandleFn
	OnSuccessFn     EventFn
	OnErrorFn       EventFn
	RetryTimeout    time.Duration
	MaxRetryTimeCnt uint8
}

func (d *DefaultHandler) Type() TaskCode {
	return d.TaskCode
}

func (d *DefaultHandler) Handle(data []byte) error {
	if d.HandleFn != nil {
		return d.HandleFn(data)
	}

	return ErrHandleFunctionIsNotSet
}

func (d *DefaultHandler) OnSuccess(data []byte) {
	if d.OnSuccessFn != nil {
		d.OnSuccessFn(data)
	}
}

func (d *DefaultHandler) OnError(data []byte) {
	if d.OnErrorFn != nil {
		d.OnErrorFn(data)
	}
}

func (d *DefaultHandler) Timeout() time.Duration {
	return d.RetryTimeout
}

func (d *DefaultHandler) MaxTimeCnt() uint8 {
	return d.MaxRetryTimeCnt
}

type TaskCode string

type TaskStatus uint8

const (
	ToDo TaskStatus = iota
	Done
	Error
)

type Task struct {
	Id        uuid.UUID       `json:"id"`
	Code      TaskCode        `json:"code"`
	Status    TaskStatus      `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	RetryOn   time.Time       `json:"retry_on"`
	RetryCnt  uint8           `json:"retry_cnt"`
	Data      []byte          `json:"data"`
	Errors    map[uint8]error `json:"errors"`
}
