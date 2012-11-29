// This is a quick adapter to ensure that only the desired interfaces are
// exported, and to define the appstore interface.
package masternoderpc

import (
	"rssproto"
)

type MasterNodeInterface interface {
	Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error
	Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error
    Join(args *rssproto.JoinArgs, reply *rssproto.JoinReply) error
    FireEpoch(args *rssproto.JoinArgs, reply *rssproto.JoinReply) error
}

type MasterNodeRPC struct {
	ms MasterNodeInterface
}

func NewMasterNodeRPC(mn MasterNodeInterface) *MasterNodeRPC {
	return &MasterNodeRPC{mn}
}

func (msrpc *MasterNodeRPC) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
	return msrpc.ms.Subscribe(args, reply)
}

func (mnrpc *MasterNodeRPC) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
	return mnrpc.mn.Unsubscribe(args, reply)
}

func (mnrpc *MasterNodeRPC) Join(args *rssproto.JoinArgs, reply *rssproto.JoinReply) error {
	return mnrpc.mn.Join(args, reply)
}

func (mnrpc *MasterNodeRPC) Ping(args *rssproto.PingArgs, reply *rssproto.PingReply) error {
	return mnrpc.mn.Ping(args, reply)
}
