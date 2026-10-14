// Command echo demonstrates a unary Connect RPC over a native libp2p stream.
package main

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/burdiyan/connectlibp2p-go"
	"github.com/burdiyan/connectlibp2p-go/examples/echo/gen/echov1"
	"github.com/burdiyan/connectlibp2p-go/examples/echo/gen/echov1/echov1connect"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type echoService struct{}

func (echoService) Echo(_ context.Context, request *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	return &echov1.EchoResponse{Message: "hello " + request.GetMessage()}, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	serverHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableRelay())
	if err != nil {
		panic(err)
	}
	defer func() { _ = serverHost.Close() }()
	clientHost, err := libp2p.New(libp2p.NoListenAddrs, libp2p.DisableRelay())
	if err != nil {
		panic(err)
	}
	defer func() { _ = clientHost.Close() }()
	if err := clientHost.Connect(ctx, peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}); err != nil {
		panic(err)
	}

	server := connect.NewServer()
	echov1connect.RegisterEchoServiceHandler(server, echoService{})
	rpcID := protocol.ID("/example/echo/connectlibp2p/1.0.0")
	stop := connectlibp2p.Serve(ctx, serverHost, rpcID, server)
	defer stop()

	client := echov1connect.NewEchoServiceClient(connect.NewClient(
		connectlibp2p.NewTransport(clientHost, serverHost.ID(), rpcID),
	))
	response, err := client.Echo(ctx, &echov1.EchoRequest{Message: "libp2p"})
	if err != nil {
		panic(err)
	}
	fmt.Println(response.GetMessage())
}
