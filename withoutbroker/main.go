package main

import (
	"log"
	"mq/withoutbroker/pub"
	"mq/withoutbroker/sub"
	"time"
)

func main() {
	pubAddr := "127.0.0.1:6600"
	publisher, err := pub.New(pubAddr)
	logErr(err)

	subscriberA := sub.New(0)
	err = subscriberA.Subscribe(pubAddr)
	logErr(err)

	subscriberB := sub.New(1)
	err = subscriberB.Subscribe(pubAddr)
	logErr(err)

	// wait for both succeed to sub
	time.Sleep(1 * time.Second)

	err = publisher.Publish([]byte("Hello World"))
	logErr(err)

	err = subscriberB.Unsubscribe(pubAddr)
	logErr(err)

	// for publisher, Publish and removeConn has race condition, which cause error if closed conn not removed in time
	// this can be solved by using a buf chan in publisher and handle publish and receive through this chan
	// here just sleep a while for simple
	time.Sleep(1 * time.Second)

	err = publisher.Publish([]byte("Who there"))
	logErr(err)

	err = subscriberA.Unsubscribe(pubAddr)
	logErr(err)

	time.Sleep(1 * time.Second)

	err = publisher.Publish([]byte("Who there again"))
	logErr(err)

	publisher.Stop()

	time.Sleep(1 * time.Second)
}

func logErr(err error) {
	if err != nil {
		log.Println(err)
	}
}
