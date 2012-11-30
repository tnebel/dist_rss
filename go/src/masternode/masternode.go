package masternode

import (
    "rssproto"
    "fmt"
    "time"
    "sync"
    "net/rpc"
)

const (
    RETRIES = 5
)

type MasterNode struct {
    lock *sync.RWMutex
    Connection *rpc.Client
    Addr string
    CallerId uint32
}

func NewMaster(port int) *MasterNode {
    mn := new(MasterNode)
    mn.lock = new(sync.RWMutex)
    return mn
}

func (mn *MasterNode) Join(args *rssproto.JoinArgs, reply *rssproto.JoinReply) error {
    mn.Addr = args.Callback
    mn.CallerId = args.CallerId
    mn.Connection = nil

    reply.Status = rssproto.OK
    return nil
}

func (mn *MasterNode) Ping(args *rssproto.PingArgs, reply *rssproto.PingReply) error {
    reply.Status = rssproto.OK
    return nil
}

func (mn *MasterNode) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    conn, err := RssStoreConnect(mn)
    if err != nil {
        reply.Status = rssproto.NOCONNECTION
        return err
    }
    err = conn.Call("RssStoreRPC.Subscribe", args, reply)
    if err != nil {
        reply.Status = rssproto.NOCONNECTION
        return err
    }
    return nil
}

func (mn *MasterNode) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    conn, err := RssStoreConnect(mn)
    if err != nil {
        reply.Status = rssproto.NOCONNECTION
        return err
    }
    err = conn.Call("RssStoreRPC.Unsubscribe", args, reply)
    if err != nil {
        reply.Status = rssproto.NOCONNECTION
        return err
    }
    return nil
}

func RssStoreConnect(mn *MasterNode) (*rpc.Client, error) {
    if mn.Connection != nil {
        return mn.Connection, nil
    }
    var err error
    for i := 0; i < RETRIES; i++ {
        if i != 0 {
            time.Sleep(time.Second)
        }
        var srvconn *rpc.Client
        srvconn, err = rpc.DialHTTP("tcp", mn.Addr)
        if err == nil {
            mn.Connection = srvconn
            return srvconn, nil
        }
    }
    fmt.Printf("Could not connect after %d attempts", RETRIES)
    return nil, err
}
