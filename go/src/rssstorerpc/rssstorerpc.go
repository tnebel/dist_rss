package rssstorerpc

import (
    "rssproto"
)

type RssStoreInterface interface {
    Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error
    Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error
    GetServers(args *rssproto.GetServersArgs, reply *rssproto.RegisterReply) error
    RegisterServer(args *rssproto.RegisterArgs, reply *rssproto.RegisterReply) error
}

type RssStoreRPC struct {
    rs RssStoreInterface
}

func NewRssStoreRPC(rs RssStoreInterface) *RssStoreRPC {
    return &RssStoreRPC{rs}
}

func (rsrpc *RssStoreRPC) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    return rsrpc.rs.Subscribe(args, reply)
}

func (rsrpc *RssStoreRPC) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    return rsrpc.rs.Unsubscribe(args, reply)
}

func (rsrpc *RssStoreRPC) GetServers(args *rssproto.GetServersArgs, reply *rssproto.RegisterReply) error {
    return rsrpc.rs.GetServers(args, reply)
}

func (rsrpc *RssStoreRPC) RegisterServer(args *rssproto.RegisterArgs, reply *rssproto.RegisterReply) error {
    return rsrpc.rs.RegisterServer(args, reply)
}
