package rssproto

import (
    "hash/fnv"
)

const (
    _ = iota
    OK
    SUBSUCCESS
    UNSUBSUCCESS
    UNSUBFAIL
    NOCONNECTION
)

type SubscribeArgs struct {
    Email string
    URI string
}

type SubscribeReply struct {
    Status int
}

type JoinArgs struct {
    CallerId uint32
    Callback string
}

type JoinReply struct {
    Status int
}

type PingArgs struct {
    CallderId uint32
}

type PingReply struct {
    Status int
}

func Hash(key []byte) uint32 {
    h := fnv.New32()
    h.Write(key)
    return h.Sum32()
}

// RegisterReply is sent in response to both Register and GetServers
type RegisterArgs struct {
    ServerInfo Node
}

type RegisterReply struct {
    Ready bool
    Servers []Node 
}

type Node struct {
     HostPort string
     NodeID uint32
}

type GetServersArgs struct {
}
