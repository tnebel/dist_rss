// This is a quick adapter to ensure that only the desired interfaces are
// exported, and to define the appstore interface.
package masternoderpc

import (
	"rssproto"
)

type MasterNodeInterface interface {
	Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error
	Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error
    Ping(args *rssproto.PingArgs, reply *rssproto.PingReply) error
}

type MasterNodeRPC struct {
	mn MasterNodeInterface
}

func NewMasterNodeRPC(mn MasterNodeInterface) *MasterNodeRPC {
	return &MasterNodeRPC{mn}
}

func (mnrpc *MasterNodeRPC) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
	return mnrpc.mn.Subscribe(args, reply)
}

func (mnrpc *MasterNodeRPC) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
	return mnrpc.mn.Unsubscribe(args, reply)
}

func (mnrpc *MasterNodeRPC) Ping(args *rssproto.PingArgs, reply *rssproto.PingReply) error {
	return mnrpc.mn.Ping(args, reply)
}
