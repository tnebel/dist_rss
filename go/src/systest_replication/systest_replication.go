package main

import (
    "masternode"
    "fmt"
    "flag"
    "rssproto"
    "log"
    //"regexp"
    "os/exec"
    "strconv"
    "time"
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
var pidP1, pidP2, pidB1, pidB2, pidS1 int

func initMaster(storage, server, myhostport string ) bool {
    mn = masternode.NewMaster(5001, storage)
    if mn == nil {
        fmt.Println("Could not start master node/app logic")
        return false
    }
    return true
}

func setup(addrToPidMap map[string]int) bool {
    initialized := initMaster("localhost:5002", "localhost:5001", "localhost:5001")
    if !initialized{
        return false
    }
    // Make call to get servers
    var reply rssproto.RegisterReply
    mn.GetServerInfo(&reply)
    primaryNodes := reply.PrimaryServers
    backupNodes := reply.BackupServers
    spareNodes := reply.SpareServers
    pidP1 = addrToPidMap[primaryNodes[0].HostPort]
    pidP2 = addrToPidMap[primaryNodes[1].HostPort]
    if backupNodes[0].NodeID == primaryNodes[0].NodeID {
        pidB1 = addrToPidMap[backupNodes[0].HostPort]
        pidB2 = addrToPidMap[backupNodes[1].HostPort]
    } else {
        pidB1 = addrToPidMap[backupNodes[1].HostPort]
        pidB2 = addrToPidMap[backupNodes[0].HostPort]
    }
    pidS1 = addrToPidMap[spareNodes[0].HostPort]
    return true
}

// setup some state by making subscriptions
// kill both primary nodes
// check that state is still as expected
func testFailover() {
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    kill(pidP1)
    kill(pidP2)
    status = subscribe(mn, EMAIL2, URI2, true)
    status = subscribe(mn, EMAIL3, URI2, true)
    status = subscribe(mn, EMAIL1, URI2, true)
    status = subscribe(mn, EMAIL1, URI1, false)
    checkStatus(rssproto.UNSUBSUCCESS, status, true)
}

func kill(pid int) {
    kill := exec.Command("kill", "-9", strconv.Itoa(pid))
    fmt.Println("Doing kill on " + strconv.Itoa(pid))
    err := kill.Start()
    if err != nil {
        log.Fatal(err)
    }
    kill.Wait()
    time.Sleep(time.Second)
}

// setup state
// kill primary
// kill new primary, now spare will be in place as primary
// check that the spare(new primary) has expected state
func testUseSpare() {
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI2, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI3, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    kill(pidP1)
    time.Sleep(time.Second)
    subscribe(mn, EMAIL1, URI2, true)
    subscribe(mn, EMAIL2, URI2, true)
    subscribe(mn, EMAIL3, URI2, true)
    kill(pidB1)
    status = subscribe(mn, EMAIL1, URI1, false)
    checkStatus(rssproto.UNSUBSUCCESS, status, true)
}

// kill the spare
// see that it doesn't mess up too much stuff
func testKillSpare() {
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI2, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI3, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    // kill the spare
    kill(pidS1)
    status = subscribe(mn, EMAIL1, URI1, false)
    if !checkStatus(rssproto.UNSUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI2, false)
    if !checkStatus(rssproto.UNSUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI3, false)
    checkStatus(rssproto.UNSUBSUCCESS, status, true)
}


// kill the backup
// kill primary
// check state
func testKillBackupAndSpare() {
    status := subscribe(mn, EMAIL1, URI1, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI2, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI3, true)
    if !checkStatus(rssproto.SUBSUCCESS, status, false) {
        return
    }
    // kill the spare
    kill(pidB1)
    kill(pidB2)
    kill(pidS1)
    status = subscribe(mn, EMAIL1, URI1, false)
    if !checkStatus(rssproto.UNSUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI2, false)
    if !checkStatus(rssproto.UNSUBSUCCESS, status, false) {
        return
    }
    status = subscribe(mn, EMAIL1, URI3, false)
    checkStatus(rssproto.UNSUBSUCCESS, status, true)
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

	tests := []TestFunc{
		TestFunc{"testFailover", testFailover},
		TestFunc{"testUseSpare", testUseSpare},
		TestFunc{"testKillSpare", testKillSpare},
		TestFunc{"testKillBackupAndSpare", testKillBackupAndSpare}}

	flag.Parse()
    // First, set up a map from addresses to PIDs given in args
    args := flag.Args()
    if (len(args)<10){
        fmt.Println("Not enough args given. Need address and pid for 5 servers.")
    }
    addrToPidMap := make(map[string]int)
    for i:=0; i<5; i=i+1 {
        // we expect addr1 pid1 addr2 pid2, etc.
        addrToPidMap[args[2*i]], _ = strconv.Atoi(args[2*i+1])
    }
    whichTestToRun := args[10]

    // Run tests
    if !setup(addrToPidMap) {
        fmt.Println("Setup did not work")
        return
    }
    var testNum int
    testNum, _ = strconv.Atoi(whichTestToRun)
    t := tests[testNum]
    fmt.Println("Starting " + t.name + ":")
    t.f()
    /*
	for _, t := range tests {
        if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
            fmt.Println("Starting " + t.name + ":")
            t.f()
        }
    }
    */

	fmt.Printf("Passed (%d/%d) tests\n", passCount, passCount + failCount)
}
