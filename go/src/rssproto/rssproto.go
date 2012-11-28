package rssproto

import (
    "hash/fnv"
    "math"
    "math/big"
    "math/rand"
    crand "crypto/rand"
)

const (
    _ = iota
    SUBSUCCESS
    UNSUBSUCCESS
    UNSUBFAIL
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

Join Reply struct {
    Status int
}

func Hash(key []byte) uint32 {
    h := fnv.New32()
    h.Write(key)
    return h.Sum32()
}
