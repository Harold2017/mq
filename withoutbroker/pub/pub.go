package pub

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

type Publisher struct {
	addr  string
	conns map[string]net.Conn
	sync.RWMutex
	stop chan struct{}
}

func New(addr string) (*Publisher, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	pub := &Publisher{
		addr:    addr,
		conns:   make(map[string]net.Conn),
		RWMutex: sync.RWMutex{},
		stop:    make(chan struct{}),
	}
	go func() {
		defer l.Close()
		for {
			c, err := l.Accept()
			if err != nil {
				log.Println("[ Publisher ] accept conn error: ", err.Error())
				return
			}
			err = pub.addConn(c)
			if err != nil {
				log.Println("[ Publisher ] add conn error: ", err.Error())
				return
			}
			// notice: not receive data from subscriber, just log it
			log.Println("[ Publisher ] received subscription from: ", c.RemoteAddr())
			go pub.receive(c)
		}
	}()

	return pub, nil
}

func (pub *Publisher) receive(conn net.Conn) {
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
				log.Println("[ Publisher ] recv:", data, "from:", conn.RemoteAddr())
				if data == "bye bye" {
					if err = pub.removeConn(conn.RemoteAddr().String()); err != nil {
						log.Println(err)
					}
					log.Println("[ Publisher ] close conn with", conn.RemoteAddr())
					return
				}
			default:
				log.Println("[ Publisher ] recv error: ", err.Error())
				return
			}
		}
	}()
}

func (pub *Publisher) addConn(conn net.Conn) error {
	if pub.isStopped() {
		return errors.New("publisher stopped")
	}
	addr := conn.RemoteAddr().String()
	pub.RLock()
	_, found := pub.conns[addr]
	pub.RUnlock()
	if found {
		return nil
	}
	pub.Lock()
	pub.conns[addr] = conn
	pub.Unlock()
	return nil
}

func (pub *Publisher) removeConn(addr string) error {
	if pub.isStopped() {
		return errors.New("publisher stopped")
	}
	pub.Lock()
	delete(pub.conns, addr)
	pub.Unlock()
	return nil
}

func (pub *Publisher) isStopped() bool {
	select {
	case <-pub.stop:
		return true
	default:
		return false
	}
}

func (pub *Publisher) Stop() {
	pub.Lock()
	for _, conn := range pub.conns {
		_ = conn.Close()
	}
	pub.conns = nil
	pub.Unlock()
	close(pub.stop)
}

func (pub *Publisher) Publish(payload []byte) error {
	if pub.isStopped() {
		return errors.New("publisher stopped")
	}

	buf := new(bytes.Buffer)

	pub.Lock()
	for addr, conn := range pub.conns {
		if _, err := conn.Write(payload); err != nil {
			_, _ = fmt.Fprintf(buf, "%s with error: %s\n", addr, err.Error())
		}
	}
	pub.Unlock()

	s := buf.String()
	if s == "" {
		return nil
	}
	return errors.New(s)
}
