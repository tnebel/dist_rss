package main

import (
    "masternode"
    "masternoderpc"
	"net/http"
	"flag"
	"fmt"
	"net"
	"net/rpc"
)


// Core Routine
func main(){
    // Parse command-line flags
    var port int
    var rssServer string
    flag.IntVar(&port,"p",5001,"Listening Port")
    flag.StringVar(&rssServer, "s", "localhost:5002", "Storage server to get list from")
    flag.Parse()

    mn := masternode.NewMaster(port, rssServer)

    // Begin servicing RPC requests
    rpc.Register(masternoderpc.NewMasterNodeRPC(mn))
    rpc.HandleHTTP()
    l, e := net.Listen("tcp",fmt.Sprintf(":%d",port))
    if e != nil {
        fmt.Printf("Fatal Listen Error:%v\n",e)
        return
    }
    http.Serve(l, nil)
}
