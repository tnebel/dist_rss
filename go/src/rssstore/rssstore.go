package rssstore

import (
    "net/rpc"
    "math"
    "time"
    "sync"
    "os/exec"
    "rssproto"
)

const (
    RETRIES = 5
)

type RssStore struct {
    hostport string
    uriToInfo map[string]*RSSInfo
    lock *sinc.RWMutex
    NodeID uint32 
}

type RSSInfo struct {
    hash uint32
    subscriptions map[string]bool
}

func NewRssStore(master string, portnum int, nodeId uint32) (*RssStore, error) {
    rs := new(RssStore)
    rs.uriToInfo = make(map[string]*RSSInfo)
    rs.lock = new(sync.RWMutex)
}

func (rs *RssStore) Subscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) {
    uri = args.URI
    email = args.email
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

func (rs *RssStore) Unsubscribe(args *rssproto.SubscribeArgs, reply *rssproto.SubscribeReply) {
    uri = args.URI
    email = args.email
    rs.lock.Lock()
    rssInfo, ok := rs.uriToInfo[uri]
    if ok {
        delete(rssInfo.subscriptions, email)
        reply.Status = rssproto.UNSUBSUCCESS
        if len(rssInfo.subscriptions) == 0 {
            delete(ss.uriToInfo, uri)
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
            if CheckRss(uri, rssInfo) {
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

func (rs *RssStore) Join() (int, error) {
    args := new(rssproto.JoinArgs)
    args.CallerId = rs.NodeId
    args.Ballback = rs.Hostport
    var reply rssproto.JoinReply

    for i := 0; i < RETRIES; i++ {
        if i != 0 {
            time.Sleep(time.Second)
        }
        err := rs.MasterConn.Call("MasterNodeRPC.Join", args, &reply)
        if err == nil {
            return reply.Status, nil
        }
    }
    return reply.Status, err
}
