package server

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"log"
	"mq/withbroker/broker"
	pb "mq/withbroker/proto"
	"net"
)

var Broker = broker.New()

type server struct{}

func (s *server) Publish(ctx context.Context, in *pb.PubMsg) (*pb.PubResp, error) {
	if err := Broker.Publish(in.Topic, in.Payload); err != nil {
		ip := getClientIP(ctx)
		return nil, errors.New("[Server Publish] error: " + err.Error() + "\nfrom: " + ip)
	}
	return &pb.PubResp{}, nil
}

func (s *server) Subscribe(in *pb.SubMsg, stream pb.MessageQueue_SubscribeServer) error {
	sub, err := Broker.Subscribe(in.Topic)
	if err != nil {
		return errors.New("[Server Subscribe] error: " + err.Error())
	}
	defer func() {
		_ = Broker.Unsubscribe(in.Topic, sub)
	}()

	for payload := range sub {
		if err := stream.Send(&pb.SubResp{Payload: payload}); err != nil {
			return errors.New("[Server Subscribe Send] error: " + err.Error())
		}
	}

	return nil
}

func Serve(address string) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Server is listening at %v\n", l.Addr())

	s := grpc.NewServer()

	pb.RegisterMessageQueueServer(s, &server{})

	reflection.Register(s)

	return s.Serve(l)
}

func clientIP(ctx context.Context) (string, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("[getClinetIP] invoke FromContext() failed")
	}
	if pr.Addr == net.Addr(nil) {
		return "", errors.New("[getClientIP] peer.Addr is nil")
	}
	return pr.Addr.String(), nil
}

func getClientIP(ctx context.Context) string {
	ip, err := clientIP(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	return ip
}
