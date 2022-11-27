package main

import (
	"fmt"
)

type Subscriber struct {
	Notifications chan any
}

func NewSubscriber() (s Subscriber, stop func()) {
	n := make(chan any)
	return Subscriber{Notifications: n}, func() { close(n) }
}

func (s Subscriber) OnUpdate(data RowData) {
	s.Notifications <- formatMsg("update", data)
}

func (s Subscriber) OnDelete(data RowData) {
	s.Notifications <- formatMsg("delete", data)
}

func (s Subscriber) OnInsert(data RowData) {
	s.Notifications <- formatMsg("insert", data)
}

func formatMsg(action string, d RowData) map[string]any {
	return map[string]any{
		"action": action,
		"table":  fmt.Sprintf("%s.%s", d.Namespace, d.TableName),
		"data":   d.Values,
	}
}
