package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/waku-org/waku-go-bindings/waku"
	"github.com/waku-org/waku-go-bindings/waku/common"
	"go.uber.org/zap"
)

const BootstrapNode = "/dns4/waku-test.bloxy.one/tcp/30304/p2p/16Uiu2HAmSZbDB7CusdRhgkD81VssRjQV5ZH13FbzCGcdnbbh6VwZ"

const AppName = "example"
const AppVersion = "1"

var ContentTopic = fmt.Sprintf("/%s/%s/foo/plain", AppName, AppVersion)

func main() {
	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to create logger: %v\n", err)
		return
	}
	defer logger.Sync()

	const requestTimeout = 30 * time.Second
	// Configure dialer node

	// Configure receiver node
	receiverNodeWakuConfig := common.WakuConfig{
		Relay:           true,
		LogLevel:        "DEBUG",
		Discv5Discovery: false,
		ClusterID:       42,
		Shards:          []uint16{0},
		Discv5UdpPort:   9021,
		TcpPort:         60021,
	}

	// Create and start receiver node
	receiverNode, err := waku.NewWakuNode(&receiverNodeWakuConfig, "receiverNode")
	if err != nil {
		fmt.Printf("Failed to create receiver node: %v\n", err)
		return
	}
	if err := receiverNode.Start(); err != nil {
		fmt.Printf("Failed to start receiver node: %v\n", err)
		return
	}
	defer receiverNode.Stop()
	time.Sleep(1 * time.Second)

	// Get receiver node's multiaddress
	receiverMultiaddr, err := receiverNode.ListenAddresses()
	if err != nil {
		fmt.Printf("Failed to get receiver node addresses: %v\n", err)
		return
	}

	receiverPeerCount, err := receiverNode.GetNumConnectedPeers()
	if err != nil {
		fmt.Printf("Failed to get receiver peer count: %v\n", err)
		return
	}
	fmt.Printf("Receiver initial peer count: %d\n", receiverPeerCount)

	receiverPeerCount, err = receiverNode.GetNumConnectedPeers()
	if err != nil {
		fmt.Printf("Failed to get receiver peer count: %v\n", err)
		return
	}
	fmt.Printf("Receiver final peer count: %d\n", receiverPeerCount)

	publicNode, err := multiaddr.NewMultiaddr(BootstrapNode)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = receiverNode.Connect(ctx, publicNode)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for envelope := range receiverNode.MsgChan {
			if envelope.Message().ContentTopic == ContentTopic {
				fmt.Printf("Received message: %s\n", string(envelope.Message().Payload))
			}
		}
	}()

	fmt.Println(receiverMultiaddr)

	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel, syscall.SIGINT, syscall.SIGTERM)
	<-quitChannel
}
