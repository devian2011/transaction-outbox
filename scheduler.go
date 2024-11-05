package transaction_outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

func newID() uuid.UUID {
	id, _ := uuid.NewV7()

	return id
}

var (
	ErrHandlerAlreadyExists      = errors.New("handler for task already exists")
	ErrHandlerForTaskIsNotExists = errors.New("errors for task is not exists")
	ErrConfiguration             = errors.New("configuration err")

	ErrMaxRetryCntReached = errors.New("max retry count reached")
)

type HandleFn func(data []byte) error
type EventFn func(data []byte)

type Handler interface {
	Type() TaskCode
	Handle(data []byte) error
	OnSuccess(data []byte)
	OnError(data []byte)
	Timeout() time.Duration
	MaxTimeCnt() uint8
}

type Scheduler struct {
	handlers         map[TaskCode]Handler
	taskCheckTimeout time.Duration
	ctx              context.Context
	wg               *sync.WaitGroup
	store            Store
	taskCh           chan Task
	errCh            chan<- error
	threadCnt        uint8
}

func NewScheduler(ctx context.Context, store Store, threadCnt uint8, taskCheckTimeout time.Duration, errCh chan<- error) (*Scheduler, error) {
	if threadCnt == 0 {
		return nil, errors.Join(errors.New("thread cnt must be gte 1"), ErrConfiguration)
	}

	if store == nil {
		return nil, errors.Join(errors.New("store is required"), ErrConfiguration)
	}

	return &Scheduler{
		ctx:              ctx,
		wg:               &sync.WaitGroup{},
		store:            store,
		threadCnt:        threadCnt,
		taskCheckTimeout: taskCheckTimeout,
		handlers:         make(map[TaskCode]Handler),
		taskCh:           make(chan Task, threadCnt),
		errCh:            errCh,
	}, nil
}

func (s *Scheduler) Register(handler Handler) error {
	if _, exists := s.handlers[handler.Type()]; exists {
		return errors.Join(fmt.Errorf("task type: %s", handler.Type()), ErrHandlerAlreadyExists)
	}

	s.handlers[handler.Type()] = handler

	return nil
}

func (s *Scheduler) Add(task Task) error {
	return s.store.Save(task)
}

func (s *Scheduler) Run() {
	for c := uint8(0); c < s.threadCnt; c++ {
		s.wg.Add(1)
		go s.do()
	}

	go s.extract()
}

func (s *Scheduler) extract() {
loop:
	for {
		select {
		case <-s.ctx.Done():
			break loop
		case <-time.After(s.taskCheckTimeout):
			tasks, taskGetErr := s.store.ToDoTasks()
			if taskGetErr != nil {
				s.errCh <- taskGetErr
				continue
			}
			now := time.Now()
			for i := range tasks {
				if tasks[i].RetryOn.Before(now) {
					s.taskCh <- tasks[i]
				}
			}
		}
	}
	close(s.taskCh)
}

func (s *Scheduler) do() {
	for v := range s.taskCh {
		if h, exists := s.handlers[v.Code]; exists {
			result := h.Handle(v.Data)
			if result != nil {
				v.UpdatedAt = time.Now()

				v.RetryCnt += 1

				if v.Errors == nil {
					v.Errors = map[uint8]error{}
				}

				v.Errors[v.RetryCnt] = result

				if v.RetryCnt >= h.MaxTimeCnt() {
					v.Status = Error
				} else {
					v.Status = ToDo
					v.RetryOn = time.Now().Add(h.Timeout())
				}

				h.OnError(v.Data)

			} else {
				v.Status = Done

				h.OnSuccess(v.Data)
			}

			if saveErr := s.store.Save(v); saveErr != nil {
				s.errCh <- saveErr
				continue
			}
		} else {
			s.errCh <- errors.Join(fmt.Errorf("task code: %s", v.Code), ErrHandlerForTaskIsNotExists)
		}
	}
	s.wg.Done()
}

func (s *Scheduler) Wait() {
	s.wg.Wait()
}
