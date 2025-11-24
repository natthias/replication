package main

import (
	proto "AuctionServer/grpc"
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AuctionServer struct {
	proto.UnimplementedAuctionNodeServer
	proto.UnimplementedAuctionServer
	timestamp     int64
	port          string
	leaderPort    string
	nodes         map[string]*proto.AuctionNodeClient
	listener      net.Listener
	srv           *grpc.Server
	isLeader      bool
	heartbeatCh   chan struct{}
	mutex         sync.Mutex
	bidders       map[string]int64
	currentWinner string
	isAuctioning  bool
	timeLimit     time.Duration
}

func main() {
	node := AuctionServer{
		timestamp:     0,
		port:          "",
		leaderPort:    ":6969",
		nodes:         make(map[string]*proto.AuctionNodeClient),
		isLeader:      false,
		heartbeatCh:   make(chan struct{}, 1),
		bidders:       make(map[string]int64),
		currentWinner: "",
		isAuctioning:  false,
		timeLimit:     30,
	}

	node.nodes[":6970"] = nil
	node.nodes[":6971"] = nil
	node.getPortAndListener()

	node.srv = grpc.NewServer()
	proto.RegisterAuctionServer(node.srv, &node)
	proto.RegisterAuctionNodeServer(node.srv, &node)
	if node.isLeader {
		log.Printf("Got port %s, that means I am the leader", node.port)
	} else {
		log.Printf("Got port %s, that means I am a follower", node.port)
	}

	go func() {
		if err := node.srv.Serve(node.listener); err != nil {
			log.Fatal(err)
		}
	}()

	for {
		for port, client := range node.nodes {
			if client != nil {
				continue
			}
			conn, err := grpc.NewClient(port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			c := proto.NewAuctionNodeClient(conn)
			node.nodes[port] = &c
		}
		if node.allNodesOnline() {
			break
		}
		time.Sleep(time.Second)
	}

	if node.isLeader {
		go node.heartbeatHelper()
	} else {
		go node.listenForHeartbeat()
	}

	fmt.Println("Usage:")
	fmt.Println("    start  start an auction (only works on the leader node)")
	fmt.Println("    crash  crash this node")
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		switch sc.Text() {
		case "crash":
			panic("Ohh no, I crashed, I sure hope the system has propper replication")
		case "start":
			go node.startAuction()
		default:
			log.Println("Unknown operation")
		}
	}
}

// Send heartbeat to all nodes
func (node *AuctionServer) heartbeatHelper() {
	node.timestamp++
	for {
		for _, client := range node.nodes {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				(*client).Heartbeat(ctx, &proto.Empty{})
			}()
		}
		time.Sleep(time.Second)
	}
}

// Wait for heartbeat or elect new leader if timeout is reached
func (node *AuctionServer) listenForHeartbeat() {
	for {
		select {
		case <-node.heartbeatCh:
			continue
		case <-time.After(time.Second * 4):
			log.Println("Leader is down, panek")
			node.electLeaderHelper()
			return
		}
	}
}

// Find a valid port to bind to
func (node *AuctionServer) getPortAndListener() {
	// Start with leader port
	listener, err := net.Listen("tcp", node.leaderPort)
	if err == nil {
		node.isLeader = true
		node.port = node.leaderPort
		node.listener = listener
		return
	}

	for port := range node.nodes {
		listener, err := net.Listen("tcp", port)
		if err == nil {
			node.port = port
			node.listener = listener
			delete(node.nodes, node.port)
			return
		}
	}
	log.Fatalln("Failed to get port")
}

// Check if all nodes are online
func (node *AuctionServer) allNodesOnline() bool {
	for _, client := range node.nodes {
		if client == nil {
			return false
		}
	}
	return true
}

// Ask all nodes to elect a leader.
// This expects len(node.nodes) - 2 grants, this is susceptible to deadlocks, as no real consensus is reached
// But it does handle "no more than one failure"
func (node *AuctionServer) electLeaderHelper() {
	grants := 0
	var wg sync.WaitGroup
	node.timestamp++
	for port, client := range node.nodes {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := (*client).ElectLeader(ctx, &proto.ElectRequest{Timestamp: node.timestamp, Port: node.port})
			if err != nil {
				// If this ever happens, we fail to elect a leader
				log.Printf("%s did not respond", port)
				return
			}
			if resp.Grant {
				grants++
			}
			node.timestamp = max(node.timestamp, resp.Timestamp) + 1
		})
	}
	wg.Wait()

	if grants == len(node.nodes) {
		log.Println("Kalm, became leader")
		node.becomeLeader()
	} else {
		log.Println("Kalm, found leader (maybe)")
		node.listenForHeartbeat()
	}
}

// Move the leader port and start sending heartbeats
func (node *AuctionServer) becomeLeader() {
	node.isLeader = true
	go node.heartbeatHelper()
	oldPort := node.port
	node.port = node.leaderPort
	node.srv.Stop()
	node.srv = grpc.NewServer()
	proto.RegisterAuctionServer(node.srv, node)
	proto.RegisterAuctionNodeServer(node.srv, node)
	for {
		listener, err := net.Listen("tcp", node.port)
		if err == nil {
			node.listener = listener
			break
		}
	}

	go func() {
		if err := node.srv.Serve(node.listener); err != nil {
			log.Fatal(err)
		}
	}()

	node.timestamp++
	for _, client := range node.nodes {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			(*client).ReportLeader(ctx, &proto.ReportLeaderRequest{
				Timestamp: node.timestamp,
				Port:      oldPort,
			})
		}()
	}
}

// Replicate current winner to all followers
func (node *AuctionServer) replicateHelper(name string, bid int64) {
	var wg sync.WaitGroup

	node.timestamp++
	for port, client := range node.nodes {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := (*client).Replicate(ctx, &proto.ReplicateRequest{
				Timestamp:     node.timestamp,
				Name:          name,
				Bid:           bid,
				CurrentWinner: node.currentWinner,
			})
			if err != nil || resp.Ack != proto.BidResponse_SUCCESS {
				// If this ever happens, we fail to elect a leader
				log.Printf("%s did not respond\n", port)
				return
			}
			node.timestamp = max(node.timestamp, resp.Timestamp) + 1
		})
	}
	wg.Wait()
}

// Send a heartbeat
func (node *AuctionServer) Heartbeat(ctx context.Context, _ *proto.Empty) (*proto.Empty, error) {
	node.heartbeatCh <- struct{}{}
	return &proto.Empty{}, nil
}

// Replicate the state to all clients
func (node *AuctionServer) Replicate(ctx context.Context, req *proto.ReplicateRequest) (*proto.BidResponse, error) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.timestamp = max(node.timestamp, req.Timestamp) + 1
	node.bidders[req.Name] = req.Bid
	node.currentWinner = req.CurrentWinner
	node.timestamp++
	return &proto.BidResponse{Timestamp: node.timestamp, Ack: proto.BidResponse_SUCCESS}, nil
}

// Nominate someone to become leader if their port is lower than our port
func (node *AuctionServer) ElectLeader(ctx context.Context, req *proto.ElectRequest) (*proto.ElectResponse, error) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.timestamp = max(node.timestamp, req.Timestamp) + 1
	node.timestamp++
	if node.isLeader {
		// Should probably outright deny leadership
		return &proto.ElectResponse{Timestamp: node.timestamp, Grant: false}, nil
	} else if req.Port > node.port {
		return &proto.ElectResponse{Timestamp: node.timestamp, Grant: false}, nil
	} else {
		return &proto.ElectResponse{Timestamp: node.timestamp, Grant: true}, nil
	}
}

// Place a bid
func (node *AuctionServer) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.timestamp = max(node.timestamp, req.Timestamp) + 1
	if !node.isLeader {
		message := "I am not the leader, cannot take bids"
		log.Println(message)
		return nil, errors.New(message)
	} else if !node.isAuctioning {
		message := "No auction is running"
		log.Println(message)
		return nil, errors.New(message)
	}

	if bid, ok := node.bidders[req.Name]; ok && bid < req.Bid {
		node.bidders[req.Name] = req.Bid
	} else if !ok {
		node.bidders[req.Name] = req.Bid
	}

	if len(node.currentWinner) == 0 || node.bidders[node.currentWinner] < req.Bid {
		node.currentWinner = req.Name
	}

	node.replicateHelper(req.Name, node.bidders[req.Name])

	return &proto.BidResponse{}, nil
}

// Return the result of the action
func (node *AuctionServer) Result(ctx context.Context, req *proto.ResultRequest) (*proto.ResultResponse, error) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.timestamp = max(node.timestamp, req.Timestamp) + 1
	node.timestamp++
	return &proto.ResultResponse{
		Timestamp: node.timestamp,
		Bid:       node.bidders[node.currentWinner],
		Name:      node.currentWinner,
		IsOver:    !node.isAuctioning,
	}, nil
}

func (node *AuctionServer) ReplicateState(ctx context.Context, req *proto.ReplicateStateRequest) (*proto.BidResponse, error) {
	node.timestamp = max(node.timestamp, req.Timestamp) + 1
	node.isAuctioning = req.IsAuctioning

	// Reset auction if starting
	if req.IsAuctioning {
		node.currentWinner = ""
		node.isAuctioning = true
		node.bidders = make(map[string]int64)
	}

	go func() {
		time.Sleep(node.timeLimit * time.Second)
		node.isAuctioning = false
	}()

	node.timestamp++
	return &proto.BidResponse{
		Timestamp: node.timestamp,
		Ack:       proto.BidResponse_SUCCESS,
	}, nil
}

// Remove the new leader from the list of replicas
func (node *AuctionServer) ReportLeader(ctx context.Context, req *proto.ReportLeaderRequest) (*proto.Empty, error) {
	delete(node.nodes, req.Port)
	return &proto.Empty{}, nil
}

// Generates a random name for the auction item
func (node *AuctionServer) generateItemName() string {
	itemRunes := make([]rune, 7)
	for i := range itemRunes {
		r := rune(rand.Intn(0x9FFF-0x4E00)) + 0x4E00 // https://en.wikipedia.org/wiki/CJK_Unified_Ideographs#CJK_Unified_Ideographs_blocks
		itemRunes[i] = r
	}

	return string(itemRunes)
}

// Start an auction, and wait 30 seconds for it to conclude
func (node *AuctionServer) startAuction() {
	if !node.isLeader {
		log.Println("I am not the leader, cannot start an auction")
	} else if node.isAuctioning {
		log.Println("Auction already running, cannot start another")
		return
	}
	node.bidders = make(map[string]int64)
	node.currentWinner = ""
	node.isAuctioning = true
	item := node.generateItemName()

	node.timestamp++
	var wg sync.WaitGroup
	for port, client := range node.nodes {
		wg.Go(func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resp, err := (*client).ReplicateState(ctx, &proto.ReplicateStateRequest{
				Timestamp:    node.timestamp,
				IsAuctioning: node.isAuctioning,
			})
			if err != nil || resp.Ack != proto.BidResponse_SUCCESS {
				log.Printf("Could not replicate state to %s\n", port)
				return
			}
			node.timestamp = max(node.timestamp, resp.Timestamp) + 1
		})
	}
	wg.Wait()

	fmt.Printf("Starting auction for %s\n", item)
	time.Sleep(node.timeLimit * time.Second)
	node.isAuctioning = false
}
