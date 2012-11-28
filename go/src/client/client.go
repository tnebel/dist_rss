package main

import (
    "rnsproto"
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
    var sub bool

    flag.StringVar(&mn, "m", "localhost:5001", "host:port of master node")
    flag.BoolVar(&sub, "s", true, "true for subscribe, false for unsubscribe")
    flag.Parse()

    if flag.NArg() < 2 {
        fmt.Printf("Please supply user email and URI to subscribe to.\n", flag.NArg())
        return
    }
    if flag.NArg() > 2 {
        fmt.Printf("Too many arguments provided.\n")
        return
    }
    email := flag.Arg(0)
    monitorURI := flag.Arg(1)

    masterconn, err := makeConnection(mn)
    if err != nil {
        fmt.Println("Could not establish connection... aborting")
    }

    args := &rnsproto.SubscribeArgs{email, monitorURI, sub}
    reply := new(rnsproto.SubscribeReply)

    //TODO: define service name and method name
    //TODO: Should this function call also retry? I think not
    err = masterconn.Call("MasterServer.Subscribe",args,reply)

    if err != nil{
        fmt.Printf("ERROR: Remote Procedure Call Failed\n")
        return
    }

    switch reply.Status {
    case rnsproto.SUBSUCCESS:
        fmt.Printf("Subscription Successful\n")
    case rnsproto.SUBFAIL:
        fmt.Printf("Subscription already exists\n")
    case rnsproto.UNSUBSUCCESS:
        fmt.Printf("Unsubscribe successful\n")
    case rnsproto.UNSUBFAIL:
        fmt.Printf("Subscription does not exist\n")
    default:
        fmt.Printf("ERROR: Go debug your code!\n")
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
