#

## A Toy of In-memory Message Queue

- `withbroker`:

    Queue based on buffered chan

    Client / Server communicates based on GRPC

    see `main.go` for example

- `withoutbroker`:

    Pub / Sub based on sockets
    
    this is NOT a mq
    
    see `main.go` for example
