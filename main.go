package main

import (
	"context"
	"fmt"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	network "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func getDefaultRelays() []peer.AddrInfo {
	rawAddrs := []string{
		"/ip4/134.209.139.213/tcp/4002/p2p/12D3KooWLr6iyQ5QDrBDvkRMCLagfDcdfPLG5bhsQkhCtwjjQ5Se",
		// "/ip4/104.131.131.82/tcp/4001/p2p/12D3KooWGkNpEPYA2QKiRwySqfjvlhmoQY2Q5QX5Q5QX5Q5Q4Q",
		// "/ip4/35.201.129.130/tcp/4001/p2p/12D3KooWJXmRaQfoKCcEe4Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q",
		// "/ip4/54.213.123.123/tcp/4001/p2p/12D3KooWGf3Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q",
		// "/ip4/18.222.123.123/tcp/4001/p2p/12D3KooWDnQ5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q",
	}

	relayInfos := make([]peer.AddrInfo, 0, len(rawAddrs))

	for _, s := range rawAddrs {
		addr, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			fmt.Printf("Ошибка парсинга адреса %s: %s\n", s, err)
			continue
		}

		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			fmt.Printf("Ошибка получения AddrInfo из %s: %s\n", s, err)
			continue
		}

		relayInfos = append(relayInfos, *info)
	}
	return relayInfos
}

func main() {
	ctx := context.Background()
	h, err := libp2p.New(
		libp2p.EnableHolePunching(),
		libp2p.ForceReachabilityPrivate(),
		libp2p.EnableAutoRelayWithStaticRelays(
			getDefaultRelays(),
		),
	)
	if err != nil {
		panic(err)
	}
	kadht, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		panic(err)
	}

	for _, addr := range dht.GetDefaultBootstrapPeerAddrInfos() {
		if err := h.Connect(ctx, addr); err != nil {
			panic(fmt.Sprint("Ошибка подключения к bootstrap peer: ", err))
		}
	}

	if err := kadht.Bootstrap(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Peer ID:", h.ID())

	h.SetStreamHandler("/chat-msg/1.0.0", handleStream)
	for {
		var input string
		fmt.Print("Введите PeerId: ")
		fmt.Scan(&input)
		peer, err := kadht.FindPeer(ctx, peer.ID(input))
		if err != nil {
			panic(fmt.Sprint("Ошибка начала поиска пира в DHT: ", err))
		}
		fmt.Println("✅ Пир найден: ", peer)
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
