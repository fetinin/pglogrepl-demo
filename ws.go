package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Пропускаем любой запрос
	},
}

var clients = sync.Map{}

func RunServer(ctx context.Context, notifications chan any) {
	const addr = ":7700"

	mux := http.NewServeMux()
	mux.Handle("/listen/changes", http.HandlerFunc(notifyHandler))

	srv := http.Server{Addr: addr, Handler: mux}
	defer srv.Close()
	go broadcastNotifications(ctx, notifications)
	go func() {
		srv.ListenAndServe()
	}()
	<-ctx.Done()
}

func notifyHandler(w http.ResponseWriter, r *http.Request) {
	connection, _ := upgrader.Upgrade(w, r, nil)
	defer connection.Close()
	logf("websocket connected: %s", connection.LocalAddr())
	messages := make(chan any)
	defer close(messages)

	clients.Store(connection, messages)
	defer clients.Delete(connection)

	for msg := range messages {
		data, _ := json.Marshal(msg)
		logf("send message to %v", connection.LocalAddr())
		err := connection.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			logf("ws send err: %v", err)
			return
		}
	}
}

func broadcastNotifications(ctx context.Context, notifications chan any) {
	for notification := range notifications {
		select {
		case <-ctx.Done():
			return
		default:
		}

		clients.Range(func(key, value any) bool {
			k := key.(*websocket.Conn)
			v := value.(chan any)
			v <- notification
			logf("notify: %v", k.LocalAddr())
			return true
		})
		return
	}
}
