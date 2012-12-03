package main

import (
    "masternode"
    "rssstore"
    "net"
    "net/http"
    "net/rpc"
    "fmt"
    "flag"
    "rssproto"
    "rssstorerpc"
)

type TestFunc struct {
    name string
    f func()
}

var mn *masternode.MasterNode
var rs *rssstore.RssStore
var testRegex *string = flag.String("t", "", "test to run")
var passCount int
var failCount int

func initMaster(storage, server, myhostport string ) net.Listener {
    l, err := net.Listen("tcp", server)
    if err != nil {
        fmt.Println("listen error")
        return nil
    }

    rs, err = rssstore.NewRssStore(myhostport, 5002, 0)
    if err != nil {
        fmt.Println("Could not start rss store")
        return nil
    }
    rsrpc := rssstorerpc.NewRssStoreRPC(rs)
    rpc.Register(rsrpc)
    rpc.HandleHTTP()
    go http.Serve(l, nil)

    // TODO: are these paramaters correct?
    mn = masternode.NewMaster(5001, myhostport)
    if mn == nil {
        fmt.Println("Could not start master node/app logic")
        return nil
    }
    return l
}

func cleanupMaster(l net.Listener) {
    if l != nil {
        l.Close()
    }

    http.DefaultServeMux = http.NewServeMux()
    rpc.DefaultServer = rpc.NewServer()
    mn = nil
}

func testNonexistantRssStore() {
    if master := masternode.NewMaster(5001, "something here"); master == nil {
        fmt.Println("PASS")
        passCount++
    } else {
        fmt.Println("FAIL")
        failCount++
    }
    cleanupMaster(nil)
}

func testSubscribe() {
    email := "tmnebel@gmail.com"
    uri := "www.google.com"
    status := subscribe(mn, email, uri)
    checkStatus(rssproto.SUBSUCCESS, status)
}

func checkStatus(expected, result int) int {
    if expected == result {
        fmt.Println("PASS")
        passCount++
    } else {
        fmt.Println("FAIL")
        failCount++
    }
}

func subscribe(mn *masternode.MasterNode, email, uri string) int {
    args := &rssproto.SubscribeArgs{email, uri}
    reply := new(rssproto.SubscribeReply)

    mn.Subscribe(args, reply)
    return reply.Status
}

func main() {
	var err error
	//output = os.Stderr
	passCount = 0
	failCount = 0

    initTests := []TestFunc{
		TestFunc{"testNonexistantRssStore", testNonexistantRssStore}}
	tests := []TestFunc{
		TestFunc{"testSubscribe", testSubscribe}}

	flag.Parse()
	if (flag.NArg() < 1) {
        log.Fatal("usage:  libtest <storage master node>")
	}

    for _, t := range initTests {
        if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
            fmt.Println("Starting " + t.name + ":")
            t.f()
        }
    }

	mn := initMaster(flag.Arg(0), fmt.Sprintf("localhost:%d", *portnum), fmt.Sprintf("localhost:%d", *portnum), libstore.NONE)
	if mn == nil {
		return
	}

	// Run tests
	for _, t := range tests {
        if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
            fmt.Println("Starting " + t.name + ":")
            t.f()
        }
    }

	fmt.Printf("Passed (%d/%d) tests\n", passCount, passCount + failCount)
}
