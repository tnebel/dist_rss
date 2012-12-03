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
    initialConnection *rpc.Client
    Addr string
    CallerId uint32
    initialServer string
    idToRssStore map[uint32]*rssStore
}

type rssStore struct {
    connection *rpc.Client
    addr string
}

// port: port num we're running on
// server: address of storage server to make initial connection to
func NewMaster(port int, server string) *MasterNode {
    mn := new(MasterNode)
    mn.lock = new(sync.RWMutex)
    mn.initialServer = server
    var err error
    mn.initialConnection, err = tryConnection(server) 
    if err != nil {
        fmt.Println("Trouble making initial connection")
        return nil
    }

    err = mn.initialConnection.Call("RssStoreRPC.GetServers", args, reply)
    //TODO: set and use args and reply
    // will get a list of Node structs, make a map from id to server
    return mn
}

func (mn *MasterNode) Join(args *rssproto.JoinArgs, reply *rssproto.JoinReply) error {
    defer fmt.Println("Rss Store joined Master")
    mn.Addr = args.Callback
    mn.CallerId = args.CallerId
    mn.initialConnection = nil
    fmt.Println(fmt.Sprintf("Joined using addr %s.", mn.Addr))

    reply.Status = rssproto.OK
    return nil
}

func (mn *MasterNode) Ping(args *rssproto.PingArgs, reply *rssproto.PingReply) error {
    reply.Status = rssproto.OK
    return nil
}

func (mn *MasterNode) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    email := args.Email
    uri := args.URI
    conn, err := RssGetConnection(mn,uri)
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
    email := args.Email
    uri := args.URI
    conn, err := RssGetConnection(mn,uri)
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

// TODO: this will take an id, it will then do the connect for that id
func RssGetConnection(mn *MasterNode, uri string) (*rpc.Client, error) {
    if mn.Connection != nil {
        return mn.Connection, nil
    }
    conn, err := tryConnection(mn.initialServer)
    if err == nil {
        mn.Connection = conn
        return mn.Connection, nil
    }
    return nil, err
}

func tryConnection(hostport string) (*rpc.Client, error) {
    var err error
    for i := 0; i < RETRIES; i++ {
        if i != 0 {
            time.Sleep(time.Second)
        }
        var srvconn *rpc.Client
        srvconn, err = rpc.DialHTTP("tcp", hostport)
        if err == nil {
            return srvconn, nil
        }
    }
    fmt.Printf("Could not connect after %d attempts", RETRIES)
    return nil, err
}
