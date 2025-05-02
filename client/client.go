package main

import (
	"bufio"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"

	"github.com/gorilla/websocket"
)

func main() {
	wg := sync.WaitGroup{}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "ws", Host: "localhost:8080", Path: "/ws"}
	log.Printf("connetion: %s", u.String())

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("connect: %s", err)
	}
	defer conn.Close()

	done := make(chan struct{})
	input := make(chan string)
	reader := bufio.NewReader(os.Stdin)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			fmt.Print(string(message))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case str := <-input:
				err := conn.WriteMessage(websocket.TextMessage, []byte(str))
				if err != nil {
					log.Printf("write: %s", err)
					return
				}
			case <-interrupt:
				log.Println("interrupt")
				err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				if err != nil {
					log.Println("write close:", err)
					return
				}
			}
		}
	}()

	for {
		select {
		case <-interrupt:
			log.Println("завершение программы")
			close(input)
			wg.Wait()
			return
		default:
			str, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("read input: %s", err)
				close(input)
				wg.Wait()
				return
			}
			input <- str
		}
	}
}
