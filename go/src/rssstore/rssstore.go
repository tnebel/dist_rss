package rssstore

import (
    "time"
    "math"
    "sync"
    "os/exec"
    "rssproto"
    "masternode"
    "fmt"
    "net/rpc"
    crand "crypto/rand"
    "math/rand"
    "math/big"
    "errors"
)

const (
    RETRIES = 5
)

type RssStore struct {
    hostport string
    uriToInfo map[string]*RSSInfo
    lock *sync.RWMutex
    nodeId uint32
    portnum int
    primaryNodeList []rssproto.Node
    backupNodeList []rssproto.Node
    spareNodeList []rssproto.Node

    registered bool
    registrationSignalCh chan bool
    nodelistMutex sync.Mutex
    numPrimaryNodes int
    numSpareNodes int
    numPrimaryRegistered int
    numBackupRegistered int
    numSpareRegistered int
    backupConn *rpc.Client

    rpcClientMap map[string]*rpc.Client
    clientMapLock *sync.RWMutex

    nodeType int
}

type RSSInfo struct {
    hash uint32
    subscriptions map[string]bool
}

func seedRNG() {
    randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
    rand.Seed( randint.Int64())
}

func NewRssStore(master string, portnum int, numNodes int, numSpareNodes int) (*RssStore, error) {
    defer fmt.Println("New RssStore going")

    rs := new(RssStore)
    rs.portnum = portnum
    rs.hostport = fmt.Sprintf("localhost:%d", portnum)
    rs.numPrimaryNodes = numNodes
    rs.numSpareNodes = numSpareNodes

    rs.uriToInfo = make(map[string]*RSSInfo)
    rs.lock = new(sync.RWMutex)
    rs.registrationSignalCh = make(chan bool, 1)
    rs.nodelistMutex = sync.Mutex{}

    rs.rpcClientMap = make(map[string]*rpc.Client)
    rs.clientMapLock = &sync.RWMutex{}

    go  rs.RegisterWithMaster(master)
    return rs, nil
}


func (rs *RssStore) RegisterServer(args *rssproto.RegisterArgs, reply *rssproto.RegisterReply) error {
    rs.nodelistMutex.Lock()
    if (rs.numPrimaryRegistered < rs.numPrimaryNodes){
        // Then we will register this node as a primary node
        seedRNG()
        reply.NodeID = rand.Uint32()
        rs.primaryNodeList = append(rs.primaryNodeList, rssproto.Node{args.ServerInfo.HostPort, reply.NodeID})
        rs.numPrimaryRegistered += 1
    } else if (rs.numBackupsRegistered < rs.numPrimaryNodes){
        // Then we will register this node as a backup node
        reply.NodeID = rs.primaryNodeList[rs.numBackupsRegistered].NodeID
        rs.backupNodeList = append(rs.backupNodeList, rssproto.Node{args.ServerInfo.HostPort, reply.NodeID})
        rs.numBackupRegistered += 1
    } else {
        reply.NodeID = 0
        rs.spareNodeList = append(rs.spareNodeList, rssproto.Node{args.ServerInfo.HostPort, reply.NodeID})
        rs.numSpareRegistered += 1
    }

    bool allPrimariesRegistered = rs.numPrimaryRegistered == rs.numPrimaryNodes
    bool allBackupsRegistered = rs.numBackupsRegistered == rs.numPrimaryNodes
    bool allSparesRegistered = rs.numSparesRegistered == rs.numSpareNodes
    reply.Ready = allPrimariesRegistered && allBackupsRegistered && allSparesRegistered
    reply.PrimaryServers = rs.primaryNodeList
    reply.BackupServers = rs.backupNodeList
    reply.SpareServers = rs.spareNodeList
    rs.nodelistMutex.Unlock()
    return nil
}

func (rs *RssStore) RegisterWithMaster(master string) error {
    numTries := 0
    var client *rpc.Client
    for client == nil {
        if numTries == RETRIES {
            rs.registrationSignalCh <- false
            fmt.Println("Could not connect to server")
            return errors.New("Could not connect to server.")
        }

        cli, err := rpc.DialHTTP("tcp", master)
        if err == nil {
            client = cli
        } else {
            time.Sleep(time.Second)
            numTries += 1
        }
    }

    numTries = 0
    args := rssproto.RegisterArgs{rssproto.Node{fmt.Sprintf("localhost:%d", rs.portnum), rs.nodeId}}
    var reply rssproto.RegisterReply
    for !rs.registered {
        if numTries == RETRIES {
            fmt.Println("Could not register with server")
            return errors.New("Could not register with server.")
        }
        err := client.Call("StorageRPC.Register", &args, &reply)
        if err != nil {
            numTries += 1
            time.Sleep(time.Second)
        }
    }

    numTries = 0
    getserversArgs := rssproto.GetServersArgs{}
    var getserversReply rssproto.RegisterReply
    for !getserversReply.Ready {
        if numTries == RETRIES {
            fmt.Println("Server refused multiple RPCs.")
            return errors.New("Server refused multiple RPCs.")
        }
        err := client.Call("StorageRPC.GetServers", &getserversArgs, &getserversReply)
        if err != nil || !getserversReply.Ready {
            numTries += 1
            time.Sleep(time.Second)
        } else {
            rs.primaryNodeList = getserversReply.PrimaryServers
            rs.backupNodeList = getserversReply.BackupServers
            rs.spareNodeList = getserversReply.SpareServers
            rs.nodeId = getserversReply.NodeID
            rs.nodeType = getserversReply.NodeType
            if (rs.nodeType == rssproto.PRIMARY){
                // Then we are a primary node, and we have to initialize
                // a connection to our secondary node.
                // determine the address of our backup
                backupAddr := ""
                for i:=0; i<len(rs.backupNodeList); ++i {
                    if rs.backupNodeList[i].NodeID == rs.nodeId {
                        backupAddr = rs.backupNodeList[i].HostPort
                    }
                }
                // set up an RPC connection to that backup
                rs.backupConn, err := rs.getConnection(backupAddr)
                if err != nil {
                    fmt.Println("Could not connect to backup node on startup")
                    return err
                }
            }
        }
    }
    rs.registered = true
    rs.registrationSignalCh <- true
    return nil
}

func (rs *RssStore) GetServers(args *rssproto.GetServersArgs, reply *rssproto.RegisterReply) error {
    rs.nodelistMutex.Lock()
    if len(reply.PrimaryServers == 0) {
        // then we are obviously not ready. If this condition is true for non-master nodes,
        // then they have not managed to get servers yet. Otherwise, they are certainly ready.
        reply.Ready = false;
        return nil
    }
    bool allPrimariesRegistered = rs.numPrimaryRegistered == rs.numPrimaryNodes
    bool allBackupsRegistered = rs.numBackupsRegistered == rs.numPrimaryNodes
    bool allSparesRegistered = rs.numSparesRegistered == rs.numSpareNodes
    //note: if this node was not the master node, then these three are true
    //      by default, 
    reply.Ready = allPrimariesRegistered && allBackupsRegistered && allSparesRegistered
    reply.PrimaryServers = rs.primaryNodeList
    reply.BackupServers = rs.backupNodeList
    reply.SpareServers = rs.spareNodeList
    rs.nodelistMutex.Unlock()
    return nil
}

func (rs *RssStore) UpdateNodeType (args *rssproto.UpdateNodeTypeArgs, reply *rssproto.UpdateNodeTypeReply) error {
    // if this node is a backup node, then it should accept requests to become a primary node.
    // if this node is a spare node, then it should accept requests to become a backup node.
    // if this node is a primary node, then it should not accept node change requests.
    requestedType := args.NewNodeType
    newNodeID := args.NodeID

    rs.lock.Lock()
    rs.nodelistMutex.Lock()
    if (rs.nodeType == rssproto.BACKUP
        && requestedType == rssproto.PRIMARY
        && newNodeID == rs.nodeId){
        // Then a node is notifying us that our primary node is dead.
        // Step up and become the new primary, notify other nodes, and initiate 'repair' process
        rs.nodeType = rssproto.PRIMARY
        rs.backupConn = nil
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        var args UpdateNodeListArgs
        args.NewPrimary = rssproto.Node{rs.HostPort, rs.nodeId}
        go BroadcastUpdate(args)
        BeginRepair()
    } else if ( rs.nodeType == rssproto.SPARE && requestedType == rssproto.BACKUP ) {
        // Then a node is requesting that we become a backup node.
        // Notify the node that we are committing to being its backup.
        // Upon changing type, notify other nodes.
        rs.nodeType = rssproto.BACKUP
        rs.nodeId = newNodeID
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        var args UpdateNodeListArgs
        args.NewBackup = rssproto.Node{rs.HostPort, newNodeID}
        go BroadcastUpdate(args)
    } else {
        // then the request is outdated--the other node has made a request and is not updated
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        reply.status = rssproto.TYPECHANGEFAILURE
    }
}

func (rs *RssStore) BeginRepair () error {
    rs.lock.Lock()
    rs.nodelistMutex.Lock()
    if rs.backupConn !=nil {
        // Then the system has already been repaired.
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        return nil
    }
    for i:=0; i<len(rs.spareNodeList); ++i {
        // request node change from spares (release nodelist lock before this call)
        nextSpare := rs.spareNodeList[i]
        var args UpdateNodeTypeArgs
        var reply UpdateNodeTypeReply
        args.NewNodeType = rssproto.BACKUP
        args.NewNodeID = rs.nodeId
        //TODO do we want to unlock stuff before our rpc call? if we do, we'll have to do some stuff to ensure no other repair calls go through
        err = cli.Call("RssStoreRPC.UpdateNodeType", args, reply)
        if err != nil || reply.Status != TYPECHANGESUCCESS {
            continue
        } else {
            rs.backupConn = getConnection(nextSpare.HostPort)
            break
        }
    }
    if (rs.backupConn == nil) {
        fmt.Println("Could not establish new backup server. Out of spares.")
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        return errors.New("Could not establish new backup server")
    }
    // if we've made it to this point, we have a connection to a new backup server.
    // We have to transfer all our data to this new backup server.
    for uri, rssInfo := range rs.uriToInfo {
        for email, sub := range rssInfo.subscriptions {
            var args *rssproto.SubscribeArgs
            var reply *rssproto.SubscribeReply
            args.Email = email
            args.URI = uri
            err = rs.backupConn.Call("RssStoreRPC.Subscribe", args, reply)
            if err != nil {
                //then we already lost our backup connection. move on without a backup
                rs.backupConn = nil
                rs.lock.Unlock()
                rs.nodelistMutex.Unlock()
                return errors.New("Could not establish new backup server")
            }
        }
    }
    rs.lock.Unlock()
    rs.nodelistMutex.Unlock()
}

func (rs *RssStore) UpdateNodeLists (args *rssproto.UpdateNodeListArgs, reply *rssproto.UpdateNodeListReply) error {
    rs.nodelistMutex.Lock()
    newPrimary := args.NewPrimary
    newBackup := args.NewPrimary
    newSpare := args.NewPrimary
    if newPrimary != nil {
        for i:=0; i<len(rs.primaryNodeList); ++i{
            if rs.primaryNodeList[i].NodeID == newPrimary.NodeID{
                rs.primaryNodeList[i] = newPrimary
                continue
            }
        }
    }
    if newBackup != nil {
        // if there is a new backup, then a spare node is replacing a backup node
        for i:=0; i<len(rs.backupNodeList); ++i {
            if rs.backupNodeList[i].NodeID == newBackup.NodeID{
                rs.backupNodeList[i] = newBackup
                continue
            }
        }
        for i:=0; i<len(rs.spareNodeList); ++i {
            if rs.spareNodeList[i].HostPort == newBackup.HostPort {
                // remove this node from the spare list
                rs.spareNodeList[i] = rs.spareNodeList[len(rs.spareNodeList)-1]
                rs.spareNodeList = rs.spareNodeList[0:len(rs.spareNodeList)-1]
                continue
            }
        }
    }
    if newSpare != nil {
        rs.spareNodeList = append(rs.spareNodeList, newSpare)
    }
    rs.nodelistMutex.Unlock()
}

func (rs *RssStore) BroadcastUpdate (args *rssproto.UpdateNodeListArgs) error {
    rs.nodelistMutex.Lock()
    nodeList := append(rs.primaryNodeList, rs.backupNodeList...)
    nodeList = append(nodeList, rs.spareNodeList...)
    rs.nodelistMutex.Unlock()
    var reply UpdateNodeTypeReply
    var nextNode Node
    for i:=0; i< len(nodeList); ++i{
        nextNode = nodeList[i]
        cli, err := rs.getConnection(nextNode.HostPort)
        if err != nil {
            continue
        }
        _ = cli.Call("RssStoreRPC.UpdateNodeLists", args, reply)
    }
}

func (rs *RssStore) isRegistered() bool {
    if rs.registered == true {
        return true
    }
    registered := <-rs.registrationSignalCh
    rs.registrationSignalCh <- registered
    return registered
}

func (rs *RssStore) getConnection(serverAddr string) (*rpc.Client, error) {
    var conn *rpc.Client
    var err error

    rs.rpcClientMapLock.RLock()
    conn, ok := rs.rpcClientMap[serverAddr]
    if ok {
        rs.rpcClientMapLock.RUnlock()
        return conn, nil
    }
    rs.rpcClientMapLock.RUnlock()

    rs.rpcClientMapLock.Lock()
    for i := 0; i < RETRIES; i++ {
        if i != 0 {
            time.Sleep(time.Second)
        }
        //Check the cache again to make sure it hasn't been recently updated
        conn, ok = rs.rpcClientMap[serverAddr]
        if ok {
            rs.rpcClientMapLock.Unlock()
            return conn, nil
        }
        conn, err = rpc.DialHTTP("tcp", serverAddr)
        if err == nil {
            rs.rpcClientMap[serverAddr] = conn
            rs.rpcClientMapLock.Unlock()
            return conn, nil
        }
    }
    rs.rpcClientMapLock.Unlock()
    return nil, err
}

/* verify that the key passed really should go to this partition */
func (rs *RssStore) verifyKey(key string) bool {
    keyId := masternode.Storehash(key)
    calculatedId := determinePartitionID(keyId, rs.nodelist)
    return calculatedId == rs.nodeId
}

func determinePartitionID (keyid uint32, nodeIdList []rssproto.Node) uint32 {
    // we'll keep track of the smallest partition greater than our id, and also the min id.
    // We return the min id only if there does not exist a partition with id greater than keyid
    minId := uint32(math.MaxUint32)
    partitionId := keyid - 1
    for i:=0; i<len(nodeIdList); i++{
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

func (rs *RssStore) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) (error) {
    if !rs.isRegistered() {
        fmt.Println("Could not register with master server")
        return errors.New("Could not register with master server")
    }

    uri := args.URI
    email := args.Email

    if !rs.verifyKey(uri) {
        // wrong parition
        reply.Status = rssproto.EWRONGSERVER
        return nil
    }

    rs.lock.Lock()
    // first, we back up this change
    // if the change is successful on the backup machine, we can assume 
    // that it will be successful on this node, also
    if rs.nodeType == rssproto.PRIMARY && rs.backupConn != nil{
        err = backupConn.Call("RssStoreRPC.Subscribe", args, reply)
        if err != nil {
            // then we've lost our backup server. begin repair process.
            rs.nodelistMutex.Lock()
            rs.backupConn = nil
            rs.nodelistMutex.Unlock()
            rs.lock.Unlock()
            BeginRepair()
            rs.lock.Lock()
            if rs.backupConn != nil {
                err = backupConn.Call("RssStoreRPC.Subscribe", args, reply)
                if err != nil {
                    // Then our backup has already failed again. 
                    // We give up and move on without a backup.
                    rs.nodelistMutex.Lock()
                    rs.backupConn = nil
                    rs.nodelistMutex.Unlock()
                }
            }
        } else if reply.Status != rssproto.SUBSUCCESS{
            rs.lock.Unlock()
            return nil
        }
    }
    rssInfo, ok := rs.uriToInfo[uri]

    if !ok {
        rssInfo = new(RSSInfo)
        rssInfo.hash = 0
        rssInfo.subscriptions = make(map[string]bool)
        rs.uriToInfo[uri] = rssInfo
    }
    rssInfo.subscriptions[email] = true
    reply.Status = rssproto.SUBSUCCESS
    rs.lock.Unlock()
    return nil
}

func (rs *RssStore) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) (error) {
    if !rs.isRegistered() {
        fmt.Println("Could not register with master server")
        return errors.New("Could not register with master server")
    }

    uri := args.URI
    email := args.Email

    if !rs.verifyKey(uri) {
        // wrong parition
        reply.Status = rssproto.EWRONGSERVER
        return nil
    }

    rs.lock.Lock()

    // first, we back up this change
    // if the change is successful on the backup machine, we can assume 
    // that it will be successful on this node, also
    if rs.nodeType == rssproto.PRIMARY && rs.backupConn != nil{
        err = cli.Call("RssStoreRPC.Unsubscribe", args, reply)
        if err != nil {
            // then we've lost our backup server. begin repair process.
            rs.nodelistMutex.Lock()
            rs.backupConn = nil
            rs.nodelistMutex.Unlock()
            rs.lock.Unlock()
            BeginRepair()
            rs.lock.Lock()
            if rs.backupConn != nil {
                err = backupConn.Call("RssStoreRPC.Unsubscribe", args, reply)
                if err != nil {
                    // Then our backup has already failed again. 
                    // We give up and move on without a backup.
                    rs.nodelistMutex.Lock()
                    rs.backupConn = nil
                    rs.nodelistMutex.Unlock()
                }
            }
        } else if reply.Status != rssproto.UNSUBSUCCESS{
            rs.lock.Unlock()
            return nil
        }
    }

    rssInfo, ok := rs.uriToInfo[uri]
    if ok {
        delete(rssInfo.subscriptions, email)
        reply.Status = rssproto.UNSUBSUCCESS
        if len(rssInfo.subscriptions) == 0 {
            delete(rs.uriToInfo, uri)
        }
    } else {
        reply.Status = rssproto.UNSUBFAIL
    }
    rs.lock.Unlock()
    return nil
}

func (rs *RssStore) CheckAll() {
    for {
        rs.lock.Lock()
        for uri, rssInfo := range rs.uriToInfo {
            if CheckRSS(uri, rssInfo) {
                go rs.notify(uri, rssInfo.subscriptions)
            }
        }
        rs.lock.Unlock()
        time.Sleep(time.Duration(5) * time.Second)
    }
}

func (rs *RssStore) notify(uri string, subscriptions map[string]bool) {
    for email, _ := range subscriptions {
        // TODO: send email
        fmt.Printf("RssStore[%d] emailing %s since %s updated detected", rs.nodeId, email, uri)
    }
}

func CheckRSS(uri string, rssInfo *RSSInfo) bool {
    output, err := exec.Command("wget", "-qO-", uri).Output()

    if err != nil {
        return false
    }
    newHash := rssproto.Hash(output)
    if newHash != rssInfo.hash {
        rssInfo.hash = newHash
        return true
    }
    return false
}
