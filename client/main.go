package main

import (
	proto "AuctionServer/grpc"
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AuctionClient struct {
	timestamp int64
	name      string
	mutex     sync.Mutex
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Must specify client name as only argument")
		os.Exit(1)
	}

	client := &AuctionClient{
		name:      os.Args[1],
		timestamp: 0,
	}

	conn, err := grpc.NewClient(":6969", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln(err)
	}
	c := proto.NewAuctionClient(conn)
	sc := bufio.NewScanner(os.Stdin)

	fmt.Println("Usage:")
	fmt.Println("    bid <amount>  bid the given amount")
	fmt.Println("    result        get the result of the auction")
	for sc.Scan() {
		actions := strings.Split(sc.Text(), " ")
		switch actions[0] {
		case "bid":
			bid, err := strconv.Atoi(actions[1])
			if err != nil {
				log.Printf("Invalid bid: %s", actions[1])
				continue
			}
			go func() {
				client.mutex.Lock()
				defer client.mutex.Unlock()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				client.timestamp++
				resp, err := c.Bid(ctx, &proto.BidRequest{Timestamp: client.timestamp, Name: client.name, Bid: int64(bid)})
				if err != nil {
					log.Println("Failed to place bid")
					return
				}
				client.timestamp = max(client.timestamp, resp.Timestamp) + 1
			}()
		case "result":
			go func() {
				client.mutex.Lock()
				defer client.mutex.Unlock()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				client.timestamp++
				resp, err := c.Result(ctx, &proto.ResultRequest{Timestamp: client.timestamp})
				if err != nil {
					log.Println("Failed to get result")
					return
				}
				client.timestamp = max(client.timestamp, resp.Timestamp) + 1

				if resp.IsOver {
					log.Printf("The winner of the auction was %s, with a bid of %d\n", resp.Name, resp.Bid)
				} else {
					log.Printf("The auction is still running, the highest bid is currently from %s with %d\n", resp.Name, resp.Bid)
				}
			}()
		default:
			log.Println("Unknown operation")
		}

	}
}
