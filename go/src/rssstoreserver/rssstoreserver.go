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

    flag.StringVar(&mn, "m", "localhost:5001", "hostport of master node")
    flag.IntVar(&port, "p", 5002, "Listening port of rssstore")
    flag.Parse()

    //p := uint32(port)
    
    rs, err := rssstore.NewRssStore(mn, port, 1)

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
