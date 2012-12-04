package masternode

import (
    "rssproto"
    "fmt"
    "time"
    "sync"
    "net/rpc"
    "hash/fnv"
    "math"
)

const (
    RETRIES = 5
)

type MasterNode struct {
    initialConnection *rpc.Client
    initialServer string
    rpcClientMap map[uint32]*rpc.Client
    rpcClientMapLock *sync.RWMutex
    nodelist []rssproto.Node
}

// port: port num we're running on
// server: address of storage server to make initial connection to
func NewMaster(port int, server string) *MasterNode {
    mn := new(MasterNode)
    mn.rpcClientMapLock = new(sync.RWMutex)
    mn.initialServer = server
    var err error
    mn.initialConnection, err = tryConnection(server) 
    if err != nil {
        fmt.Println("Trouble making initial connection")
        return nil
    }
    args := new(rssproto.GetServersArgs)
    var reply rssproto.RegisterReply
    for i := 0; i < RETRIES; i++ {
        if i == 0 {
            time.Sleep(time.Second)
        }
        _ = mn.initialConnection.Call("RssStoreRPC.GetServers", args, &reply)
        if !reply.Ready {
            return nil
        }
    }
    mn.nodelist = reply.Servers
    // will get a list of Node structs, make a map from id to server
    return mn
}

func (mn *MasterNode) Ping(args *rssproto.PingArgs, reply *rssproto.PingReply) error {
    reply.Status = rssproto.OK
    return nil
}

func (mn *MasterNode) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    uri := args.URI
    cli, err := mn.getServer(uri)
    if err != nil {
        reply.Status = rssproto.NOCONNECTION
        return err
    }
    err = cli.Call("RssStoreRPC.Subscribe", args, reply)
    if err != nil {
        reply.Status = rssproto.NOCONNECTION
        return err
    }
    return nil
}

func (mn *MasterNode) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    uri := args.URI
    cli, err := mn.getServer(uri)
    if err != nil {
        reply.Status = rssproto.NOCONNECTION
        return err
    }
    err = cli.Call("RssStoreRPC.Unsubscribe", args, reply)
    if err != nil {
        reply.Status = rssproto.NOCONNECTION
        return err
    }
    return nil
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

func determinePartitionID (keyid uint32, nodeIdList []rssproto.Node) uint32 {
    // we'll keep track of the smallest partition greater than our id, and also the min id.
    // We return the min id only if there does not exist a partition with id greater than keyid
    minId := uint32(math.MaxUint32)
    partitionId := keyid - 1

    for i := 0; i < len(nodeIdList); i++ {
        nextId := nodeIdList[i].NodeID
        if nextId >= keyid && nextId - keyid < partitionId - keyid {
            partitionId = nextId
        }
        if nextId < minId {
            minId = nextId
        }
    }
    if partitionId >= keyid {
        // then there is a partition id greater than this key id
        return partitionId
    }
    // then there is no partition id greater than this key id, so we 
    // wrap around and return the smallest partition id.
    return minId
}


func (mn *MasterNode) getServer(key string) (*rpc.Client, error) {
    // Use beginning of key to group related keys together
    keyid := Storehash(key)

    partitionID := determinePartitionID(keyid, mn.nodelist)
    mn.rpcClientMapLock.RLock()
    cli, ok := mn.rpcClientMap[partitionID]
    if ok {
        mn.rpcClientMapLock.RUnlock()
        return cli, nil
    }
    mn.rpcClientMapLock.RUnlock()

    // Create this rpc client and add it to the cache
    mn.rpcClientMapLock.Lock()
    // verify that there is still no rpc client in the cache for this partition
    cli, ok = mn.rpcClientMap[partitionID]
    if ok {
        mn.rpcClientMapLock.Unlock()
        return cli, nil
    }

    hp := ""
    for i := 0; i < len(mn.nodelist); i++ {
        if mn.nodelist[i].NodeID == partitionID {
            hp = mn.nodelist[i].HostPort
            break
        }
    }
    //TODO consider checking cache again
    cli, err := rpc.DialHTTP("tcp", hp)
    if err != nil {
        return nil, err
    }
    mn.rpcClientMap[partitionID] = cli
    mn.rpcClientMapLock.Unlock()
    return cli, nil
}

func Storehash(key string) uint32 {
    hasher := fnv.New32()
    hasher.Write([]byte(key))
    return hasher.Sum32()
}
