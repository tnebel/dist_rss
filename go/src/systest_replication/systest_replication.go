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
    //"log"
    "regexp"
    "os/exec"
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
var testRegex *string = flag.String("t", "", "test to run")
var passCount int
var failCount int

func initMaster(storage, server, myhostport string ) bool {
    mn = masternode.NewMaster(5001, storage)
    if mn == nil {
        fmt.Println("Could not start master node/app logic")
        return false
    }
    return true
}

func setup() bool {
    return initMaster("localhost:5002", "localhost:5001", "localhost:5001") 
}

// setup some state by making subscriptions
// kill both primary nodes
// check that state is still as expected
func testFailover() {
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    kill(pid)
    kill(pid)
    status = subscribe(mn, EMAIL1, URI1, false)
    checkStatus(rssproto.UNSUBSUCCESS, status, true)
}

fun kill(pid int) {
    kill := exec.Command("kill -9", pid)
    err := kill.Start()
    if err != nil {
        log.Fatal(err)
    }
    kill.Wait()
}

// setup state
// kill primary
// kill new primary, now spare will be in place as primary
// check that the spare(new primary) has expected state

// kill the spare
// doesn't fuck shit up

// kill the backup
// kill primary
// check state

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

	tests := []TestFunc{
		TestFunc{"testFailover", testFailover}}

    /*
	flag.Parse()
	if (flag.NArg() < 1) {
        log.Fatal("usage:  libtest <storage master node>")
	}
    */

	// Run tests
    if !setup() {
        fmt.Println("Setup did not work")
        return
    }
	for _, t := range tests {
        if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
            fmt.Println("Starting " + t.name + ":")
            t.f()
        }
    }

	fmt.Printf("Passed (%d/%d) tests\n", passCount, passCount + failCount)
}
