package main

import (
	"context"
	"fmt"
	"strings"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/event"
	network "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

func main() {
	ctx := context.Background()
	var kadht *dht.IpfsDHT

	h, err := libp2p.New(
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.EnableAutoRelayWithPeerSource(
			func(context context.Context, num int) <-chan peer.AddrInfo {
				ch := make(chan peer.AddrInfo, num)
				go func() {
					defer close(ch)
					if kadht == nil {
						return
					}
					for _, addr := range kadht.RoutingTable().GetPeerInfos() {
						select {
						case ch <- peer.AddrInfo{ID: addr.Id}:
						case <-context.Done():
							return
						}
					}
				}()
				return ch
			},
		),
	)
	if err != nil {
		panic(err)
	}

	sub, err := h.EventBus().Subscribe([]interface{}{
		new(event.EvtLocalAddressesUpdated),
	})
	if err != nil {
		panic(err)
	}

	btsrDone := make(chan struct{})
	go func() {
		for e := range sub.Out() {
			switch v := e.(type) {
			case event.EvtLocalAddressesUpdated:
				for _, addr := range v.Current {
					if strings.Contains(addr.Address.String(), "p2p-circuit") {
						close(btsrDone)
						return
					}
				}
			}
		}
	}()

	kadht, err = dht.New(
		ctx,
		h,
		dht.Mode(dht.ModeAutoServer),
		dht.RoutingTableFilter(func(ht any, p peer.ID) bool {
			return dht.PrivateRoutingTableFilter(ht, p) || dht.PublicRoutingTableFilter(ht, p)
		}),
	)
	if err != nil {
		panic(err)
	}

	for _, addr := range dht.GetDefaultBootstrapPeerAddrInfos() {
		if err := h.Connect(ctx, addr); err != nil {
			fmt.Println("Ошибка подключения к bootstrap peer: ", err)
		}
	}

	fmt.Println("Ждем окончания процесса peer bootstrap...")
	if err := kadht.Bootstrap(ctx); err != nil {
		panic(err)
	}
	<-btsrDone

	fmt.Println("Peer ID:", h.ID())

	for _, addr := range h.Addrs() {
		if strings.Contains(addr.String(), "p2p-circuit") {
			fmt.Println(addr.String())
		}
	}

	h.SetStreamHandler("/chat-msg/1.0.0", handleStream)
	for {
		var input string
		fmt.Print("Введите PeerAddres: ")
		fmt.Scan(&input)
		addr, err := multiaddr.NewMultiaddr(input)
		if err != nil {
			fmt.Println("Ошибка преобрзования multiaddr: ", err)
			return
		}
		peerId, _ := peer.Decode(string(input[strings.LastIndex(input, "/")+1:]))
		if err := h.Connect(ctx, peer.AddrInfo{
			ID:    peerId,
			Addrs: []multiaddr.Multiaddr{addr},
		}); err != nil {
			fmt.Println("Ошибка подключения к пиру: ", err)
			return
		}
		newCtx := network.WithAllowLimitedConn(ctx, "relay connection")
		s, err := h.NewStream(newCtx, peerId, protocol.ID("/chat-msg/1.0.0"))
		if err != nil {
			fmt.Println("Ошибка создания нового потока: ", err)
			return
		}
		n, _ := s.Write([]byte("Hello!"))
		buffer := make([]byte, 1024)
		s.Read(buffer)
		fmt.Println(string(buffer[:n]))
		s.Close()
	}
}

func handleStream(s network.Stream) {
	fmt.Println("📥 New stream from:", s.Conn().RemotePeer())

	buffer := make([]byte, 1024)
	n, _ := s.Read(buffer)
	fmt.Println("Получено: ", string(buffer[:n]))
	s.Write([]byte("Hi, Im good thanks!"))
	s.Close()
}
