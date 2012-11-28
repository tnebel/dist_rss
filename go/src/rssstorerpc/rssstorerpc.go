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

func (rsrpc *AppStoreRPC) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    return rsrpc.rs.Unsubscribe(args, reply)
}
