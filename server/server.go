package main

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

type Message struct {
	Content []byte          // содержимое сообщения
	Sender  *websocket.Conn // соединение пользователя
}

type Hub struct {
	clients    map[*websocket.Conn]bool // Список подключённых клиентов
	broadcast  chan Message             // Канал для сообщений, которые нужно разослать
	register   chan *websocket.Conn     // Канал для регистрации новых клиентов
	unregister chan *websocket.Conn     // Канал для удаления клиентов
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan Message),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

func (h *Hub) run() {
	for {
		select {
		case conn := <-h.register:
			h.clients[conn] = true
			log.Printf("клиент подключён | всего клиентов: %d", len(h.clients))
		case conn := <-h.unregister:
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				conn.Close()
				log.Printf("клиент отключён | всего клиентов: %d", len(h.clients))
			}
		case message := <-h.broadcast:
			// для отправки сообщений разным людям, нужно вызывать WriteMessage у тех conn, которым хотим отправить сообщение
			for conn := range h.clients {
				if conn == message.Sender {
					continue
				}
				err := conn.WriteMessage(websocket.TextMessage, message.Content)
				if err != nil {
					log.Printf("write error: %s", err)
					delete(h.clients, conn)
					conn.Close()
				}
			}
		}
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Разрешить все источники (настройте для продакшена)
	},
}

func wsConnection(c echo.Context, hub *Hub) error {
	conn, err := upgrader.Upgrade(c.Response().Writer, c.Request(), nil)
	if err != nil {
		log.Error(err)
	}
	// каждый новый conn (подключение) - это новый пользователь.
	hub.register <- conn

	for {
		// считываем полученное сообщение
		_, message, err := conn.ReadMessage()
		if err != nil {
			// Проверяем, является ли ошибка закрытием соединения
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("Клиент %p закрыл соединение: %s", conn, err)
			} else {
				log.Printf("read error from client %p: %s", conn, err)
			}
			return nil
		}

		// Пропускаем пустые сообщения
		if len(message) == 0 {
			continue
		}

		log.Printf("message: %s", message)

		// отправляем сообщение
		hub.broadcast <- Message{Content: message, Sender: conn}
	}
}

func main() {
	e := echo.New()
	hub := newHub()
	go hub.run()

	e.GET("/ws", func(c echo.Context) error {
		return wsConnection(c, hub)
	})
	e.Start(":8080")
}
