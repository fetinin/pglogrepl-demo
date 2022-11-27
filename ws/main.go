package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Пропускаем любой запрос
	},
}

var clients = sync.Map{}

func main() {
	http.HandleFunc("/listen/changes", notifyHandler)
	go func() {
		for {
			time.Sleep(10 * time.Second)
			clients.Range(func(key, value any) bool {
				k := key.(*websocket.Conn)
				v := value.(chan string)
				v <- "hey"
				fmt.Printf("send msg to: %v\n", k.LocalAddr())
				return true
			})
		}
	}()
	http.ListenAndServe(":7700", nil)
}

func notifyHandler(w http.ResponseWriter, r *http.Request) {
	connection, _ := upgrader.Upgrade(w, r, nil)
	defer connection.Close()
	messages := make(chan string)
	defer close(messages)

	clients.Store(connection, messages)
	defer clients.Delete(connection)

	for msg := range messages {
		connection.WriteMessage(websocket.TextMessage, []byte(msg))

		mt, _, err := connection.ReadMessage()
		if err != nil || mt == websocket.CloseMessage {
			break // Выходим из цикла, если клиент пытается закрыть соединение или связь с клиентом прервана
		}
	}
}
