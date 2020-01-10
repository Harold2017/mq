package sub

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"sync"
)

type Subscriber struct {
	id    int
	conns map[string]net.Conn
	sync.RWMutex
	stop chan struct{}
}

func New(id int) *Subscriber {
	return &Subscriber{
		id:      id,
		conns:   make(map[string]net.Conn),
		RWMutex: sync.RWMutex{},
		stop:    make(chan struct{}),
	}
}

func (sub *Subscriber) isStopped() bool {
	select {
	case <-sub.stop:
		return true
	default:
		return false
	}
}

func (sub *Subscriber) Subscribe(addr string) error {
	if sub.isStopped() {
		return errors.New("publisher stopped")
	}
	sub.RLock()
	_, found := sub.conns[addr]
	sub.RUnlock()
	if found {
		return nil
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	// just use tcp keepalive for demo, in practice should send ping/pong
	err = conn.SetKeepAlive(true)
	if err != nil {
		return err
	}
	go func() {
		defer conn.Close()
		buf := make([]byte, 1024)
		r := bufio.NewReader(conn)
		for {
			rLen, err := r.Read(buf)
			data := string(buf[:rLen])
			switch err {
			case io.EOF:
				// return
			case nil:
				log.Println("[ Subscriber", sub.id, "] recv: ", data)
			default:
				// Unsubscribe will throw error here
				// log.Println("[ Subscriber", sub.id, "] recv: ", err.Error())
				return
			}
		}
	}()
	sub.Lock()
	sub.conns[addr] = conn
	sub.Unlock()
	return nil
}

func (sub *Subscriber) Unsubscribe(addr string) error {
	if sub.isStopped() {
		return errors.New("publisher stopped")
	}
	sub.RLock()
	conn, found := sub.conns[addr]
	sub.RUnlock()
	if !found {
		return nil
	}
	// say bye bye to publisher
	_, err := conn.Write([]byte("bye bye"))
	if err != nil {
		return err
	}
	err = conn.Close()
	if err != nil {
		return err
	}
	sub.Lock()
	delete(sub.conns, addr)
	sub.Unlock()
	return nil
}
