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
    numPrimariesRegistered int
    numBackupsRegistered int
    numSparesRegistered int
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

    go rs.RegisterWithMaster(master)
    go rs.CheckAll()
    return rs, nil
}


func (rs *RssStore) RegisterServer(args *rssproto.RegisterArgs, reply *rssproto.RegisterReply) error {
    fmt.Println("attempting to register server...")
    rs.nodelistMutex.Lock()
    if (rs.numPrimariesRegistered < rs.numPrimaryNodes){
        // Then we will register this node as a primary node
        seedRNG()
        reply.NodeID = rand.Uint32()
        reply.NodeType = rssproto.PRIMARY
        rs.primaryNodeList = append(rs.primaryNodeList, rssproto.Node{args.ServerInfo.HostPort, reply.NodeID})
        rs.numPrimariesRegistered += 1
    } else if (rs.numBackupsRegistered < rs.numPrimaryNodes){
        // Then we will register this node as a backup node
        reply.NodeID = rs.primaryNodeList[rs.numBackupsRegistered].NodeID
        reply.NodeType = rssproto.BACKUP
        rs.backupNodeList = append(rs.backupNodeList, rssproto.Node{args.ServerInfo.HostPort, reply.NodeID})
        rs.numBackupsRegistered += 1
    } else {
        reply.NodeID = 0
        reply.NodeType = rssproto.SPARE
        rs.spareNodeList = append(rs.spareNodeList, rssproto.Node{args.ServerInfo.HostPort, reply.NodeID})
        rs.numSparesRegistered += 1
    }

    allPrimariesRegistered := rs.numPrimariesRegistered == rs.numPrimaryNodes
    allBackupsRegistered := rs.numBackupsRegistered == rs.numPrimaryNodes
    allSparesRegistered := rs.numSparesRegistered == rs.numSpareNodes
    reply.Ready = allPrimariesRegistered && allBackupsRegistered && allSparesRegistered
    reply.PrimaryServers = rs.primaryNodeList
    reply.BackupServers = rs.backupNodeList
    reply.SpareServers = rs.spareNodeList
    rs.nodelistMutex.Unlock()

    if reply.NodeType == rssproto.PRIMARY {
        fmt.Println("Registered a server as a primary node")
    } else if reply.NodeType == rssproto.BACKUP {
        fmt.Println("Registered a server as a backup node")
    } else {
        fmt.Println("Registered a server as a spare node")
    }

    return nil
}

func (rs *RssStore) RegisterWithMaster(master string) error {
    fmt.Println("attempting to register with Master server...")
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
    var args rssproto.RegisterArgs
    args.ServerInfo = rssproto.Node{fmt.Sprintf("localhost:%d", rs.portnum), rs.nodeId}
    var reply rssproto.RegisterReply
    finishedRegistration := false
    for !finishedRegistration {
        if numTries == RETRIES {
            fmt.Println("Could not register with server")
            return errors.New("Could not register with server.")
        }
        err := client.Call("RssStoreRPC.RegisterServer", &args, &reply)
        if err != nil {
            numTries += 1
            time.Sleep(time.Second)
        } else {
            rs.nodeId = reply.NodeID
            rs.nodeType = reply.NodeType
            finishedRegistration = true
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
        err := client.Call("RssStoreRPC.GetServers", &getserversArgs, &getserversReply)
        if err != nil || !getserversReply.Ready {
            numTries += 1
            time.Sleep(time.Second)
        } else {
            rs.primaryNodeList = getserversReply.PrimaryServers
            rs.backupNodeList = getserversReply.BackupServers
            rs.spareNodeList = getserversReply.SpareServers
            if (rs.nodeType == rssproto.PRIMARY){
                // Then we are a primary node, and we have to initialize
                // a connection to our secondary node.
                // determine the address of our backup
                backupAddr := ""
                for i:=0; i<len(rs.backupNodeList); i++ {
                    if rs.backupNodeList[i].NodeID == rs.nodeId {
                        backupAddr = rs.backupNodeList[i].HostPort
                    }
                }
                // set up an RPC connection to that backup
                rs.backupConn, err = rs.getConnection(backupAddr)
                if err != nil {
                    fmt.Println("Could not connect to backup node on startup")
                    return err
                }
            }
        }
    }
    rs.registered = true
    rs.registrationSignalCh <- true
    fmt.Println("Successfully registered with master")
    return nil
}

func (rs *RssStore) GetServers(args *rssproto.GetServersArgs, reply *rssproto.RegisterReply) error {
    rs.nodelistMutex.Lock()
    if len(rs.primaryNodeList) == 0 && rs.numPrimaryNodes == 0{
        // then we are not the master node and we have not successfully called getservers
        rs.nodelistMutex.Unlock()
        reply.PrimaryServers = rs.primaryNodeList
        reply.BackupServers = rs.backupNodeList
        reply.SpareServers = rs.spareNodeList
        reply.Ready = false;
        return nil
    }
    allPrimariesRegistered := rs.numPrimariesRegistered == rs.numPrimaryNodes
    allBackupsRegistered := rs.numBackupsRegistered == rs.numPrimaryNodes
    allSparesRegistered := rs.numSparesRegistered == rs.numSpareNodes
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
    fmt.Println(fmt.Sprintf("Requested to change node types from %s to %s.", rs.nodeType, args.NewNodeType))
    // if this node is a backup node, then it should accept requests to become a primary node.
    // if this node is a spare node, then it should accept requests to become a backup node.
    // if this node is a primary node, then it should not accept node change requests.
    requestedType := args.NewNodeType
    newNodeID := args.NewNodeID

    rs.lock.Lock()
    rs.nodelistMutex.Lock()
    if (rs.nodeType == rssproto.BACKUP && requestedType == rssproto.PRIMARY && newNodeID == rs.nodeId){
        // Then a node is notifying us that our primary node is dead.
        // Step up and become the new primary, notify other nodes, and initiate 'repair' process
        rs.nodeType = rssproto.PRIMARY
        rs.backupConn = nil
        reply.Status = rssproto.TYPECHANGESUCCESS
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        var args rssproto.UpdateNodeListArgs
        args.NewPrimary = rssproto.Node{rs.hostport, rs.nodeId}
        args.NewBackup = rssproto.Node{"", 0}
        args.NewSpare = rssproto.Node{"", 0}
        go rs.BroadcastUpdate(args)
        rs.BeginRepair()
    } else if ( rs.nodeType == rssproto.SPARE && requestedType == rssproto.BACKUP ) {
        // Then a node is requesting that we become a backup node.
        // Notify the node that we are committing to being its backup.
        // Upon changing type, notify other nodes.
        rs.nodeType = rssproto.BACKUP
        reply.Status = rssproto.TYPECHANGESUCCESS
        rs.nodeId = newNodeID
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        var args rssproto.UpdateNodeListArgs
        args.NewPrimary = rssproto.Node{}
        args.NewBackup = rssproto.Node{rs.hostport, newNodeID}
        args.NewSpare = rssproto.Node{}
        go rs.BroadcastUpdate(args)
    } else {
        // then the request is outdated--the other node has made a request and is not updated
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        reply.Status = rssproto.TYPECHANGEFAILURE
    }
    return nil
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
    for i:=0; i<len(rs.spareNodeList); i++ {
        // request node change from spares (release nodelist lock before this call)
        // TODO detect dead spare nodes and broadcast change here
        fmt.Println(fmt.Sprintf("attempting to contact spare %d of %d", i+1, len(rs.spareNodeList)))
        nextSpare := rs.spareNodeList[i]
        var args rssproto.UpdateNodeTypeArgs
        var reply rssproto.UpdateNodeTypeReply
        args.NewNodeType = rssproto.BACKUP
        args.NewNodeID = rs.nodeId
        cli, err := rs.getConnection(nextSpare.HostPort)
        if err != nil {
            fmt.Println("failed to connect with this spare:")
            fmt.Println(err)
            continue
        }
        err = cli.Call("RssStoreRPC.UpdateNodeType", &args, &reply)
        if err != nil || reply.Status != rssproto.TYPECHANGESUCCESS {
            fmt.Println("Error in rpc call to update spare node type")
            fmt.Println(err)
            fmt.Println(reply.Status == rssproto.TYPECHANGESUCCESS)
            continue
        } else {
            rs.backupConn = cli
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
        for email, _ := range rssInfo.subscriptions {
            var args rssproto.SubscribeArgs
            var reply rssproto.SubscribeReply
            args.Email = email
            args.URI = uri
            err := rs.backupConn.Call("RssStoreRPC.Subscribe", &args, &reply)
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
    return nil
}

func (rs *RssStore) UpdateNodeLists (args *rssproto.UpdateNodeListArgs, reply *rssproto.UpdateNodeListReply) error {
    fmt.Println("Informed of update in node list.")
    rs.nodelistMutex.Lock()
    newPrimary := args.NewPrimary
    newBackup := args.NewBackup
    newSpare := args.NewSpare
    if newPrimary.HostPort != "" {
        for i:=0; i<len(rs.primaryNodeList); i++ {
            if rs.primaryNodeList[i].NodeID == newPrimary.NodeID{
                rs.primaryNodeList[i] = newPrimary
                continue
            }
        }
    }
    if newBackup.HostPort != ""{
        // if there is a new backup, then a spare node is replacing a backup node
        for i:=0; i<len(rs.backupNodeList); i++ {
            if rs.backupNodeList[i].NodeID == newBackup.NodeID{
                rs.backupNodeList[i] = newBackup
                continue
            }
        }
        for i:=0; i<len(rs.spareNodeList); i++ {
            if rs.spareNodeList[i].HostPort == newBackup.HostPort {
                // remove this node from the spare list
                rs.spareNodeList[i] = rs.spareNodeList[len(rs.spareNodeList)-1]
                rs.spareNodeList = rs.spareNodeList[0:len(rs.spareNodeList)-1]
                continue
            }
        }
    }
    if newSpare.HostPort != "" {
        rs.spareNodeList = append(rs.spareNodeList, newSpare)
    }
    rs.nodelistMutex.Unlock()
    return nil
}

func (rs *RssStore) BroadcastUpdate (args rssproto.UpdateNodeListArgs) error {
    fmt.Println("Broadcasting Update to node lists")
    rs.nodelistMutex.Lock()
    nodeList := append(rs.primaryNodeList, rs.backupNodeList...)
    nodeList = append(nodeList, rs.spareNodeList...)
    rs.nodelistMutex.Unlock()
    var reply rssproto.UpdateNodeTypeReply
    var nextNode rssproto.Node
    for i:=0; i< len(nodeList); i++ {
        nextNode = nodeList[i]
        cli, err := rs.getConnection(nextNode.HostPort)
        if err != nil {
            fmt.Println("Error was not nil")
            fmt.Println(err)
            continue
        }
        _ = cli.Call("RssStoreRPC.UpdateNodeLists", &args, &reply)
    }
    return nil
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

    rs.clientMapLock.RLock()
    conn, ok := rs.rpcClientMap[serverAddr]
    if ok {
        rs.clientMapLock.RUnlock()
        return conn, nil
    }
    rs.clientMapLock.RUnlock()

    rs.clientMapLock.Lock()
    for i := 0; i < RETRIES; i++ {
        if i != 0 {
            time.Sleep(time.Second)
        }
        //Check the cache again to make sure it hasn't been recently updated
        conn, ok = rs.rpcClientMap[serverAddr]
        if ok {
            rs.clientMapLock.Unlock()
            return conn, nil
        }
        conn, err = rpc.DialHTTP("tcp", serverAddr)
        if err == nil {
            rs.rpcClientMap[serverAddr] = conn
            rs.clientMapLock.Unlock()
            return conn, nil
        }
    }
    rs.clientMapLock.Unlock()
    return nil, err
}

/* verify that the key passed really should go to this partition */
func (rs *RssStore) verifyKey(key string) bool {
    keyId := masternode.Storehash(key)
    calculatedId := determinePartitionID(keyId, rs.primaryNodeList)
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
    fmt.Println(fmt.Sprintf("Received subscribe request for %s to %s", args.Email, args.URI))
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
        err := rs.backupConn.Call("RssStoreRPC.Subscribe", args, reply)
        if err != nil {
            // then we've lost our backup server. begin repair process.
            rs.nodelistMutex.Lock()
            rs.backupConn = nil
            rs.nodelistMutex.Unlock()
            rs.lock.Unlock()
            rs.BeginRepair()
            rs.lock.Lock()
            if rs.backupConn != nil {
                err = rs.backupConn.Call("RssStoreRPC.Subscribe", args, reply)
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
    //rssInfo.subscriptions[email] = true
    _, ok = rssInfo.subscriptions[email]
    if !ok {
        rssInfo.subscriptions[email] = true
        reply.Status = rssproto.SUBSUCCESS
    } else {
        // already existed
        reply.Status = rssproto.SUBFAIL
    }
    rs.lock.Unlock()
    return nil
}

func (rs *RssStore) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) (error) {
    fmt.Println(fmt.Sprintf("Received subscribe request for %s to %s", args.Email, args.URI))
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
        err := rs.backupConn.Call("RssStoreRPC.Unsubscribe", args, reply)
        if err != nil {
            // then we've lost our backup server. begin repair process.
            rs.nodelistMutex.Lock()
            rs.backupConn = nil
            rs.nodelistMutex.Unlock()
            rs.lock.Unlock()
            rs.BeginRepair()
            rs.lock.Lock()
            if rs.backupConn != nil {
                err = rs.backupConn.Call("RssStoreRPC.Unsubscribe", args, reply)
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
        rs.nodelistMutex.Lock()
        if rs.nodeType == rssproto.PRIMARY {
            for uri, rssInfo := range rs.uriToInfo {
                if CheckRSS(uri, rssInfo) {
                    go rs.notify(uri, rssInfo.subscriptions)
                }
            }
        }
        rs.lock.Unlock()
        rs.nodelistMutex.Unlock()
        time.Sleep(time.Duration(5) * time.Second)
    }
}

func (rs *RssStore) notify(uri string, subscriptions map[string]bool) {
    for email, _ := range subscriptions {
        // TODO: send email
        fmt.Printf("RssStore[%d] emailing %s since %s updated detected\n", rs.nodeId, email, uri)
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
