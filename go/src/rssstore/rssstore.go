package rssstore

import (
    "time"
    "math"
    "sync"
    "os/exec"
    "rssproto"
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
    NodeID uint32
    //backupConn *rpc.Client
    portnum int
    nodelist []rssproto.Node

    registered bool
    registrationSignalCh chan bool
    registrationMutex sync.Mutex
    numnodes int
    numregistered int
}

type RSSInfo struct {
    hash uint32
    subscriptions map[string]bool
}

func seedRNG() {
    randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
    rand.Seed( randint.Int64())
}


func NewRssStore(master string, portnum int, nodeId uint32, numNodes int) (*RssStore, error) {
    defer fmt.Println("New RssStore going")

    rs := new(RssStore)
    rs.portnum = portnum
    rs.hostport = fmt.Sprintf("localhost:%d", portnum)
    rs.numnodes = numNodes
    if rs.numnodes == 0 {
        rs.numnodes = 1
    }

    //TODO when we add in backups and spares, this will be determined by the master node initially.
    seedRNG()
    rs.NodeID = rand.Uint32()

    rs.uriToInfo = make(map[string]*RSSInfo)
    rs.lock = new(sync.RWMutex)

    go  rs.RegisterWithMaster(master)
    return rs, nil
}


func (rs *RssStore) RegisterServer(args *rssproto.RegisterArgs, reply *rssproto.RegisterReply) error {
    rs.registrationMutex.Lock()
    //TODO we'll decide right here whether this node is a primary, backup, or spare node
    //TODO this means we'll have to designate the id of the registering node, instead of letting it self-assign
    rs.nodelist = append(rs.nodelist, args.ServerInfo)
    rs.numregistered += 1
    rs.registrationMutex.Unlock()
    reply.Ready = rs.numregistered == rs.numnodes
    reply.Servers = rs.nodelist
    return nil
}

func (rs *RssStore) RegisterWithMaster(master string) error {
    //TODO we'll determine our nodeId in here.
    //TODO listen for list of spares, also
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
    args := rssproto.RegisterArgs{rssproto.Node{fmt.Sprintf("localhost:%d", rs.portnum), rs.NodeID}}
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
        } else {
            rs.registered = true
            rs.registrationSignalCh <- true
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
            rs.nodelist = getserversReply.Servers
        }
    }

    return nil
}

func (rs *RssStore) GetServers(args *rssproto.GetServersArgs, reply *rssproto.RegisterReply) error {
    reply.Ready = rs.numregistered == rs.numnodes
    reply.Servers = rs.nodelist
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

func connectToServer(serverAddr string) (*rpc.Client, error) {
    var conn *rpc.Client
    var err error
    for i := 0; i < RETRIES; i++ {
        if i != 0 {
            time.Sleep(time.Second)
        }
        conn, err = rpc.DialHTTP("tcp", serverAddr)
        if err == nil {
            return conn, nil
        }
    }
    return nil, err
}

func (rs *RssStore) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) (error) {
    if !rs.isRegistered() {
        fmt.Println("Could not register with master server")
        return errors.New("Could not register with master server")
    }

    uri := args.URI
    email := args.Email
    rs.lock.Lock()
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
    rs.lock.Lock()
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
        fmt.Printf("RssStore[%d] emailing %s since %s updated detected", rs.NodeID, email, uri)
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

/*
We shouldn't need this function anymore
func (rs *RssStore) Join() (int, error) {
    args := new(rssproto.JoinArgs)
    args.CallerId = rs.NodeID
    args.Callback = rs.hostport
    var reply rssproto.JoinReply
    var err error
    for i := 0; i < RETRIES; i++ {
        if i != 0 {
            time.Sleep(time.Second)
        }
        err = rs.masterConn.Call("MasterNodeRPC.Join", args, &reply)
        if err == nil {
            return reply.Status, nil
        }
    }
    fmt.Println("Rss stored joined master")
    return reply.Status, err
}
*/
