package rssstorerpc

import (
    "rssproto"
)

type RssStoreInterface interface {
    Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error
    Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error
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

func (rsrpc *RssStoreRPC) UpdateNodeType (args *rssproto.UpdateNodeTypeArgs, reply *rssproto.UpdateNodeTypeReply) error {
    return rspc.rs.UpdateNodeType(args, reply)
}

func (rsrpc *RssStoreRPC) UpdateNodeLists (args *rssproto.UpdateNodeListArgs, reply *rssproto.UpdateNodeListReply) error {
    return rspc.rs.UpdateNodeLists(args, reply)
}

