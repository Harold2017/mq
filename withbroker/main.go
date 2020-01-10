package main

import (
	"log"
	"mq/withbroker/client"
	"mq/withbroker/server"
	"time"
)

func main() {
	serverAddr := "127.0.0.1:8800"
	go func() {
		err := server.Serve(serverAddr)
		if err != nil {
			log.Fatalln(err)
		}
	}()

	for {
		if _, err := client.ListServices(serverAddr); err != nil {
			log.Println(err)
			time.Sleep(100 * time.Millisecond)
		} else {
			log.Println("Server is ready")
			break
		}
	}

	topic := "test"
	payload := []byte("test")

	c := client.New(serverAddr)
	err := c.Subscribe(topic)
	if err != nil {
		log.Println(err)
	}

	c1 := client.New(serverAddr)
	err = c1.Subscribe(topic)
	if err != nil {
		log.Println(err)
	}

	time.Sleep(10 * time.Millisecond)

	err = c.Publish(topic, payload)
	if err != nil {
		log.Println(err)
	}

	time.Sleep(10 * time.Millisecond)

	log.Println("......")

	msg, err := c1.ReadMsg(topic)
	if err != nil {
		log.Println(err)
	}
	if msg != string(payload) {
		log.Println(msg)
	} else {
		log.Println("successfully recv: ", msg)
	}
}
