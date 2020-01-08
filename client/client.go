package client

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	pb "mq/proto"
	"sync"
	"time"
)

// DefaultReadMsgTimeout ...
const DefaultReadMsgTimeout =  10 * time.Millisecond

type Client struct {
	serverAddr string

	sync.RWMutex

	subChans map[string]chan []byte

	stop chan struct{}
}

func New(serverAddr string) *Client {
	return &Client{
		serverAddr: serverAddr,
		RWMutex:    sync.RWMutex{},
		subChans:   make(map[string]chan []byte),
		stop:       make(chan struct{}),
	}
}

func (c *Client) isStopped() bool {
	select {
	case <-c.stop:
		return true
	default:
		return false
	}
}

func (c *Client) Publish(topic string, payload []byte) error {
	if c.isStopped() {
		return errors.New("client stopped")
	}
	conn, err := grpc.Dial(c.serverAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewMessageQueueClient(conn)
	_, err = client.Publish(context.TODO(), &pb.PubMsg{
		Topic:                topic,
		Payload:              payload,
	})

	return err
}

func (c *Client) Subscribe(topic string) error {
	if c.isStopped() {
		return errors.New("client stopped")
	}
	conn, err := grpc.Dial(c.serverAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	// close conn when client stopped
	go func() {
		select {
		case <-c.stop:
			conn.Close()
		}
	}()

	client := pb.NewMessageQueueClient(conn)
	sub, err := client.Subscribe(context.TODO(), &pb.SubMsg{Topic:topic})
	if err != nil {
		return err
	}

	subChan := make(chan []byte, 10)

	c.RLock()
	_, found := c.subChans[topic]
	c.RUnlock()
	if !found {
		c.Lock()
		// cache 10 messages
		c.subChans[topic] = subChan
		c.Unlock()
	} else {
		// already subscribed
		return nil
	}

	go func() {
		for {
			resp, err := sub.Recv()
			if err != nil {
				// close when error
				conn.Close()
				return
			}

			select {
			case subChan <- resp.Payload:
				// recv
			case <-c.stop:
				return
			}
		}
	}()

	return nil
}

func (c *Client) Unsubscribe(topic string) error {
	if c.isStopped() {
		return errors.New("client stopped")
	}

	c.RLock()
	subChan, found := c.subChans[topic]
	c.RUnlock()
	if !found {
		return nil
	}
	c.Lock()
	close(subChan)
	delete(c.subChans, topic)
	c.Unlock()
	return nil
}

func (c *Client) Stop() error {
	if c.isStopped() {
		return nil
	}
	close(c.stop)
	c.Lock()
	c.subChans = nil
	c.Unlock()
	return nil
}

func (c *Client) ReadMsg(topic string) (string, error) {
	sub, found := c.subChans[topic]
	if !found {
		return "", errors.New("not subscribe to this topic")
	}
	for {
		select {
		case str := <-sub:
			return string(str), nil
		case <-time.After(DefaultReadMsgTimeout):
			return "", errors.New("timeout")
		default:
			// do nothing
		}
	}
}

func ListServices(serverAddr string) (string, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()
	rc := rpb.NewServerReflectionClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := rc.ServerReflectionInfo(ctx)
	if err != nil {
		return "", err
	}
	err = r.Send(&rpb.ServerReflectionRequest{MessageRequest: &rpb.ServerReflectionRequest_ListServices{ListServices: "*"}})
	if err == nil {
		resp, err := r.Recv()
		if err == nil {
			return fmt.Sprint(resp.MessageResponse), nil
		}
	}
	return "", nil
}
