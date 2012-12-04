package masternode

import (
    "rssproto"
    "fmt"
    "errors"
    "time"
    "sync"
    "net/rpc"
    "hash/fnv"
    "math"
    crand "crypto/rand"
    "math/rand"
    "math/big"

)

const (
    RETRIES = 5
)

type MasterNode struct {
    initialConnection *rpc.Client
    initialServer string
    rpcClientMap map[string]*rpc.Client
    rpcClientMapLock *sync.RWMutex

    primaryNodeList []rssproto.Node
    backupNodeList []rssproto.Node
    spareNodeList []rssproto.Node
    nodelistMutex sync.Mutex
}

func seedRNG() {
    randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
    rand.Seed( randint.Int64())
}

// port: port num we're running on
// server: address of storage server to make initial connection to
func NewMaster(port int, server string) *MasterNode {
    mn := new(MasterNode)
    mn.rpcClientMapLock = new(sync.RWMutex)
    mn.rpcClientMap = make(map[string]*rpc.Client)
    mn.nodelistMutex = sync.Mutex{}

    mn.initialServer = server
    seedRNG()
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
    mn.nodelistMutex.Lock()
    mn.primaryNodeList = reply.PrimaryServers
    mn.backupNodeList = reply.BackupServers
    mn.spareNodeList = reply.SpareServers
    mn.nodelistMutex.Unlock()
    return mn
}

func (mn *MasterNode) RetreiveServerLists() error {
    mn.nodelistMutex.Lock()
    nodeList := append(mn.primaryNodeList, mn.backupNodeList...)
    nodeList = append(nodeList, mn.spareNodeList...)
    mn.nodelistMutex.Unlock()

    for numTries := 0; numTries < RETRIES; numTries++{
        rand_server_idx := rand.Intn(len(nodeList))
        serverAddr := nodeList[rand_server_idx].HostPort
        conn, err := mn.getServer(serverAddr)
        if err != nil {
            numTries += 1
            continue
        }
        args := new(rssproto.GetServersArgs)
        var reply rssproto.RegisterReply
        _ = conn.Call("RssStoreRPC.GetServers", args, &reply)
        if !reply.Ready {
            numTries += 1
            continue
        }
        mn.nodelistMutex.Lock()
        mn.primaryNodeList = reply.PrimaryServers
        mn.backupNodeList = reply.BackupServers
        mn.spareNodeList = reply.SpareServers
        mn.nodelistMutex.Unlock()
        return nil
    }
    return errors.New("could not establish a connection with any servers to update list")
}

func (mn *MasterNode) Ping(args *rssproto.PingArgs, reply *rssproto.PingReply) error {
    reply.Status = rssproto.OK
    return nil
}

func (mn *MasterNode) NotifyBackupOfFailure(partitionId uint32) {
    mn.nodelistMutex.Lock()
    for i:=0; i<len(mn.backupNodeList); i++ {
        if mn.backupNodeList[i].NodeID == partitionId {
            cli, err := mn.getServer(mn.backupNodeList[i].HostPort)
            if (err != nil){
                // instead of panicing, return and hope that the a call 
                // to retreive servers will save us in the future
                return
            }
            var args rssproto.UpdateNodeTypeArgs
            var reply rssproto.UpdateNodeTypeReply
            args.NewNodeType = rssproto.PRIMARY
            args.NewNodeID = partitionId
            // if this call gives an error, it is likely because another app
            // node has already updated it.
            _ = cli.Call("RssStoreRPC.UpdateNodeType", args, reply)
            mn.nodelistMutex.Unlock()
            return
        }
    }

    mn.nodelistMutex.Unlock()
}

func (mn *MasterNode) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    uri := args.URI
    for numTries:=0; numTries<RETRIES; numTries++ {
        p_id, hp := determinePartitionID(Storehash(uri), mn.primaryNodeList)
        cli, err := mn.getServer(hp)

        if err != nil {
            mn.NotifyBackupOfFailure(p_id)
            time.Sleep(time.Second)
            mn.RetreiveServerLists()
            numTries += 1
            continue
        }
        err = cli.Call("RssStoreRPC.Subscribe", args, reply)
        if err != nil {
            mn.NotifyBackupOfFailure(p_id)
            time.Sleep(time.Second)
            mn.RetreiveServerLists()
            numTries += 1
            continue
        } else{
            return nil
        }
    }
    reply.Status = rssproto.NOCONNECTION
    return errors.New("could not reach primary node after multiple attemts to update list")
}

func (mn *MasterNode) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) error {
    uri := args.URI

    for numTries:=0; numTries<RETRIES; numTries++ {
        p_id, hp := determinePartitionID(Storehash(uri), mn.primaryNodeList)
        cli, err := mn.getServer(hp)

        if err != nil {
            mn.NotifyBackupOfFailure(p_id)
            time.Sleep(time.Second)
            mn.RetreiveServerLists()
            numTries += 1
            continue
        }
        err = cli.Call("RssStoreRPC.Unsubscribe", args, reply)
        if err != nil {
            mn.NotifyBackupOfFailure(p_id)
            time.Sleep(time.Second)
            mn.RetreiveServerLists()
            numTries += 1
            continue
        } else {
            return nil
        }
    }
    reply.Status = rssproto.NOCONNECTION
    return errors.New("could not reach primary node after multiple attemts to update list")
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

func determinePartitionID (keyid uint32, nodeIdList []rssproto.Node) (uint32, string) {
    // returns the partition id and hostport string of the partition responsible for the given key.
    // we'll keep track of the smallest partition greater than our id, and also the min id.
    // We return the min id only if there does not exist a partition with id greater than keyid
    minId := uint32(math.MaxUint32)
    hp := ""
    minHp := ""
    partitionId := keyid - 1

    for i := 0; i < len(nodeIdList); i++ {
        nextId := nodeIdList[i].NodeID
        if nextId >= keyid && nextId - keyid < partitionId - keyid {
            partitionId = nextId
            hp = nodeIdList[i].HostPort
        }
        if nextId < minId {
            minId = nextId
            minHp = nodeIdList[i].HostPort
        }
    }
    if partitionId >= keyid {
        // then there is a partition id greater than this key id
        return partitionId, hp
    }
    // then there is no partition id greater than this key id, so we 
    // wrap around and return the smallest partition id.
    return minId, minHp
}

func (mn *MasterNode) getServer(serverAddr string) (*rpc.Client, error) {
    // Use beginning of key to group related keys together
    mn.rpcClientMapLock.RLock()
    cli, ok := mn.rpcClientMap[serverAddr]
    if ok {
        mn.rpcClientMapLock.RUnlock()
        return cli, nil
    }
    mn.rpcClientMapLock.RUnlock()

    // Create this rpc client and add it to the cache
    mn.rpcClientMapLock.Lock()
    // verify that there is still no rpc client in the cache for this partition
    cli, ok = mn.rpcClientMap[serverAddr]
    if ok {
        mn.rpcClientMapLock.Unlock()
        return cli, nil
    }

    //TODO consider checking cache again
    cli, err := rpc.DialHTTP("tcp", serverAddr)
    if err != nil {
        return nil, err
    }
    mn.rpcClientMap[serverAddr] = cli
    mn.rpcClientMapLock.Unlock()
    return cli, nil
}

func Storehash(key string) uint32 {
    hasher := fnv.New32()
    hasher.Write([]byte(key))
    return hasher.Sum32()
}
