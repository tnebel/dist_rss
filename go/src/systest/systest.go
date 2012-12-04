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
    //"log"
    "regexp"
)

const (
    EMAIL1 = "test1@test.com"
    EMAIL2 = "test2@test.com"
    EMAIL3 = "test3@test.com"
    URI1 = "www.gmail.com"
    URI2 = "www.facebook.com"
    URI3 = "www.reddit.com"
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
var lis net.Listener
var lis2 net.Listener

func initMaster(storage, server, myhostport string ) net.Listener {
    l := initRssStore(storage, 5002, 0) 
    if l == nil {
        return nil
    }
    mn = masternode.NewMaster(5001, storage)
    if mn == nil {
        fmt.Println("Could not start master node/app logic")
        return nil
    }
    return l
}

func initMasterWithTwoRssStores(storage, server, myhostport string ) net.Listener {
    l := initTwoRssStore("locahost:5002", "localhost:5003")
    if l == nil {
        return nil
    }
    mn = masternode.NewMaster(5001, storage)
    if mn == nil {
        fmt.Println("Could not start master node/app logic")
        return nil
    }
    return l
}

func initRssStore(storage string, masterport, numNodes int) net.Listener {
    l, err := net.Listen("tcp", storage)
    if err != nil {
        fmt.Println("listen error")
        fmt.Println(err)
        return nil
    }

    rs, err = rssstore.NewRssStore(storage, masterport, numNodes)
    if err != nil {
        fmt.Println("Could not start rss store")
        return nil
    }
    rsrpc := rssstorerpc.NewRssStoreRPC(rs)
    rpc.Register(rsrpc)
    rpc.HandleHTTP()
    go http.Serve(l, nil)
    return l
}

func initTwoRssStore(masterStorage, secondStorage string) net.Listener {
    l1 := initRssStore(masterStorage, 5002, 2) 
    l2 := initRssStore(secondStorage, 5002, 0)
    if l1 != nil && l2 != nil {
        return l1
    }
    return nil
}

func cleanupMaster(l net.Listener) {
    if l != nil {
        l.Close()
    }

    http.DefaultServeMux = http.NewServeMux()
    rpc.DefaultServer = rpc.NewServer()
    mn = nil
}

func setup() {
    lis = initMaster("localhost:5002", "localhost:5001", "localhost:5001") 
}

func setup2() {
    lis = initMasterWithTwoRssStores("localhost:5002", "localhost:5001", "localhost:5001") 
}

func testNonexistantRssStore() {
    if master := masternode.NewMaster(5001, "localhost:5002"); master == nil {
        fmt.Println("PASS")
        passCount++
    } else {
        fmt.Println("FAIL")
        failCount++
    }
    cleanupMaster(nil)
}

func testSubscribe() {
    setup()
    defer cleanupMaster(lis)
    status := subscribe(mn, EMAIL1, URI1, true)
    checkStatus(rssproto.SUBSUCCESS, status, true)
}

func testMultSubscribe() {
    setup()
    defer cleanupMaster(lis)
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return 
    }
    status = subscribe(mn, EMAIL2, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL3, URI1, true)
    checkStatus(rssproto.SUBSUCCESS, status, true) 
}

func testMultSubscribeDiffURI() {
    setup()
    defer cleanupMaster(lis)
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return 
    }
    status = subscribe(mn, EMAIL2, URI2, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL3, URI3, true)
    checkStatus(rssproto.SUBSUCCESS, status, true) 
}

func testUnsubscribe1() {
    setup()
    defer cleanupMaster(lis)
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI1, false)
    checkStatus(rssproto.UNSUBSUCCESS, status, true)
}

func testUnsubscribe2() {
    setup()
    defer cleanupMaster(lis)
    status := subscribe(mn, EMAIL1, URI1, false)
    checkStatus(rssproto.UNSUBFAIL, status, true)
}

func testUnsubscribe3() {
    setup()
    defer cleanupMaster(lis)
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI2, false)
    if !checkStatus(rssproto.UNSUBFAIL, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI1, false)
    checkStatus(rssproto.UNSUBSUCCESS, status, true)
}

func testSubTwoRssStores() {
    setup2()
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return 
    }
    status = subscribe(mn, EMAIL2, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL3, URI1, true)
    checkStatus(rssproto.SUBSUCCESS, status, true) 
}


// final == true means that this is the final call to checkStatus
// within a test, thus if expected == result, log the PASS
// a expected != result always logs as a FAIL
func checkStatus(expected, result int, final bool) bool {
    if expected == result {
        if final {
            fmt.Println("PASS")
            passCount++
        }
        return true
    }
    fmt.Println("FAIL")
    failCount++
    return false
}

// pass true to subscribe param to do sub, or false for unsub
func subscribe(mn *masternode.MasterNode, email, uri string, subscribe bool) int {
    args := &rssproto.SubscribeArgs{email, uri}
    reply := new(rssproto.SubscribeReply)
    if subscribe {
        mn.Subscribe(args, reply)
    } else {
        mn.Unsubscribe(args, reply)
    }
    return reply.Status
}

func main() {
	//var err error
	//output = os.Stderr
	passCount = 0
	failCount = 0

    initTests := []TestFunc{
		TestFunc{"testNonexistantRssStore", testNonexistantRssStore}}
	tests := []TestFunc{
		TestFunc{"testSubscribe", testSubscribe},
		TestFunc{"testMultSubscribe", testMultSubscribe},
		TestFunc{"testMultSubscribeDiffURI", testMultSubscribeDiffURI},
		TestFunc{"testUnsubscribe1", testUnsubscribe1},
		TestFunc{"testUnsubscribe2", testUnsubscribe2},
		TestFunc{"testUnsubscribe3", testUnsubscribe3}}
	//	TestFunc{"testSubTwoRssStores", testSubTwoRssStores}}

    /*
	flag.Parse()
	if (flag.NArg() < 1) {
        log.Fatal("usage:  libtest <storage master node>")
	}
    */

    for _, t := range initTests {
        if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
            fmt.Println("Starting " + t.name + ":")
            //t.f()
        }
    }

	//mn := initMaster(flag.Arg(0), fmt.Sprintf("localhost:%d", *portnum), fmt.Sprintf("localhost:%d", *portnum))
    /*
    lis = initMaster("localhost:5002", "localhost:5001", "localhost:5001") 
	if lis == nil {
		return
	}
    */

	// Run tests
	for _, t := range tests {
        if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
            fmt.Println("Starting " + t.name + ":")
            t.f()
        }
    }

	fmt.Printf("Passed (%d/%d) tests\n", passCount, passCount + failCount)
}
