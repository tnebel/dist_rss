package main

import (
    "net/http"
    "flag"
    "fmt"
    "net"
    "net/rpc"
    "rssstore"
    "rssstorerpc"
)

func main() {
    var mn string
    var port int
    var numNodes int

    flag.StringVar(&mn, "m", "localhost:5002", "hostport of master storage node")
    flag.IntVar(&port, "p", 5002, "Listening port of rssstore")
    flag.IntVar(&numNodes, "N", 0, "Become the master.  Specifies the number of nodes in the system, including the master.")

    flag.Parse()

    rs, err := rssstore.NewRssStore(mn, port, numNodes)

    if err != nil {
        fmt.Printf("Error in creating rss store")
        return
    }

    rpc.Register(rssstorerpc.NewRssStoreRPC(rs))
    rpc.HandleHTTP()
    handle, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
    if err != nil {
        fmt.Println("Error listening for rssstore")
        return
    }
    http.Serve(handle, nil)
}
