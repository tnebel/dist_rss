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
    flag.IntVar(&port,"p",5001,"Listening Port")
    flag.Parse()

    mn := master.NewMaster(port)

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
