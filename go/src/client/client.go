package main

import (
    "rssproto"
    "net/rpc"
    "fmt"
    "flag"
    "time"
    "errors"
)

const(
    CONN_RETRIES = 5
)

func main(){
    var mn string
    var sub int

    flag.StringVar(&mn, "m", "localhost:5001", "host:port of master node")
    flag.IntVar(&sub, "s", 1, "1 for subscribe, 0 for unsubscribe")
    flag.Parse()

    if flag.NArg() < 2 {
        fmt.Printf("Please supply user email and URI to subscribe to.\n", flag.NArg())
        return
    }
    if flag.NArg() > 2 { fmt.Printf("Too many arguments provided.\n")
        return
    }
    email := flag.Arg(0)
    uri := flag.Arg(1)

    masterconn, err := makeConnection(mn)
    if err != nil {
        fmt.Println("Could not establish connection... aborting")
        return
    }

    args := &rssproto.SubscribeArgs{email, uri}
    reply := new(rssproto.SubscribeReply)

    if sub > 0 {
        err = masterconn.Call("MasterNodeRPC.Subscribe", args, reply)
    } else {
        err = masterconn.Call("MasterNodeRPC.Unsubscribe", args, reply)
    }

    if err != nil{
        fmt.Printf("ERROR: Remote Procedure Call Failed\n")
        fmt.Println(err)
        return
    }

    switch reply.Status {
    case rssproto.SUBSUCCESS:
        fmt.Printf("Subscription Successful\n")
    case rssproto.SUBFAIL:
        fmt.Printf("Subscription already existed\n")
    case rssproto.UNSUBSUCCESS:
        fmt.Printf("Unsubscribe successful\n")
    case rssproto.UNSUBFAIL:
        fmt.Printf("Subscription does not exist\n")
    default:
        fmt.Printf("ERROR: unrecognized status. Should receive success/fail status.\n")
    }
}

func makeConnection(mn string) (*rpc.Client, error) {
    var masterconn *rpc.Client
	var err error
	for i := 0 ; i < CONN_RETRIES; i++ {
		masterconn, err = rpc.DialHTTP("tcp", mn)
        if err == nil {
			break
	    }
        fmt.Printf("Failed on an attempt to connect to master node %s\n",mn)
		time.Sleep(time.Second)
	}
    if err != nil{
        fmt.Printf("Connection failed after %d attempts\n", CONN_RETRIES)
        return nil, errors.New("Could not establish connection")
    }
    return masterconn, nil
}
