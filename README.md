# Replication
This is a poor mans implementation of leader-centric replication for the ITU Distributed Systems course.
It assumes a perfect network where no packets are dropped and supports *at most* one crash.
Leader selection is done by finding the node with the lowest port.

## Generating grpc
1. run `protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/proto.proto` in the root of the project

## Running the program
1. Open a terminal and navigate to the node directory (e.g. `cd node`)
1. Start the three nodes by running the appropriate command (`go run main.go`) 3 times.
   This likely requires three terminals
1. Start a new terminal and navigate to the client directory (e.g. `cd client`)
1. Start client by runng the appropriate command (`go run main.go <username>`)

## Usage
node:
```
Usage:
    start  start an auction (only works on the leader node)
    crash  crash this node
```

client:
```
Usage:
    bid <amount>  bid the given amount
    result        get the result of the auction
```
