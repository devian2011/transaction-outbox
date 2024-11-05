# Transaction outbox pattern implementation

## Installation

```shell
go get -u github.com/devian2011/transaction-outbox
```

## How to

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	
	"github.com/google/uuid"

	outbox "github.com/devian2011/transaction-outbox"
)

const (
	TaskCodeSendMail outbox.TaskCode = "task_send_mail"
)

type taskSendMailData struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Title string `json:"title"`
	Body  string `json:"body"`
}

func newTransactionOutboxHandler() outbox.Handler {
	
	unmarshalTaskSendMailData := func(data []byte) *taskSendMailData {
		entity := &taskSendMailData{}
		_ = json.Unmarshal(data, entity)

		return entity
	}

	sendMail := func(from, to, title, body string) error {
		return nil
	}

	return &outbox.DefaultHandler{
		TaskCode: TaskCodeSendMail,
		HandleFn: func(data []byte) error {
			entity := unmarshalTaskSendMailData(data)
			return sendMail(entity.From, entity.To, entity.Title, entity.Body)
		},
		OnSuccessFn: func(data []byte) {
			entity := unmarshalTaskSendMailData(data)
			fmt.Printf("E-mail successfully send from: %s to: %s\n", entity.From, entity.To)
		},
		OnErrorFn: func(data []byte) {
			entity := unmarshalTaskSendMailData(data)
			fmt.Printf("E-mail didn't send from: %s to: %s\n", entity.From, entity.To)
		},
		RetryTimeout:    5 * time.Minute, // Time between try
		MaxRetryTimeCnt: 3,               // Max try count
	}
}

func newStore() outbox.Store {
	store := outbox.NewMemoryStore()
	store.SetDoneTaskCnt(10)  // Show only last 10 done tasks
	store.SetErrorTaskCnt(10) // Show only last 10 error tasks

	return store
}

func main() {
	errCh := make(chan error)

	scheduler, _ := outbox.NewScheduler(
		context.Background(),
		newStore(),    // Storage for tasks
		4,             // Count of background threads for make tasks
		5*time.Second, // Time delay between check tasks for handle
		errCh,         // Global err chanel
	)

	if registerErr := scheduler.Register(newTransactionOutboxHandler()); registerErr != nil {
		log.Fatal(registerErr)
	}

	scheduler.Run()

	id, _ := uuid.NewV7()
	data, _ := json.Marshal(taskSendEmailData{
		From: "from@email",
		To: "to@email",
		Title: "title@email",
		Body: "body@email",
    })

	// Send task
	scheduler.Add(outbox.Task{
		Id:        id,
		Code:      TaskCodeSendMail,
		Status:    outbox.ToDo,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		RetryOn:   time.Now(),
		RetryCnt:  0,
		Data:      data,
		Errors:    map[uint8]error{},
	})

	scheduler.Wait()
}

```
