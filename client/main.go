package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/hungys/swimring/util"
	"github.com/olekukonko/tablewriter"
)

const (
	GetCmd    = "get"
	PutCmd    = "put"
	DeleteCmd = "del"
	StatCmd   = "stat"
	ExitCmd   = "exit"
)

const (
	// ONE is the weakest consistency level.
	// For read request, returns value when the first response arrived.
	// For write request, returns when the first ACK received.
	ONE = "ONE"
	// QUORUM is the moderate consistency level.
	// For read request, returns value when the quorum set of replicas all responded.
	// For write request, returns when the quorum set of replicas all responded ACKs.
	QUORUM = "QUORUM"
	// ALL is the strongest consistency level.
	// For read request, returns value when all replicas responded.
	// For write request, returns when all replicas all responded ACKs.
	ALL = "ALL"
	// GetOp is the name of the service method for Get.
	GetOp = "SwimRing.Get"
	// PutOp is the name of the service method for Put.
	PutOp = "SwimRing.Put"
	// DeleteOp is the name of the service method for Delete.
	DeleteOp = "SwimRing.Delete"
	// StatOp is the name of the service method for Stat.
	StatOp = "SwimRing.Stat"
)

// SwimringClient is a RPC client for connecting to SwimRing server.
type SwimringClient struct {
	address string
	port    int
	client  *rpc.Client

	readLevel  string
	writeLevel string
}

// GetRequest is the payload of Get.
type GetRequest struct {
	Level string
	Key   string
}

// GetResponse is the payload of the response of Get.
type GetResponse struct {
	Key, Value string
}

// PutRequest is the payload of Put.
type PutRequest struct {
	Level      string
	Key, Value string
}

// PutResponse is the payload of the response of Put.
type PutResponse struct{}

// DeleteRequest is the payload of Delete.
type DeleteRequest struct {
	Level string
	Key   string
}

// DeleteResponse is the payload of the response of Delete.
type DeleteResponse struct{}

// StateRequest is the payload of Stat.
type StateRequest struct{}

// StateResponse is the payload of the response of Stat.
type StateResponse struct {
	Nodes []NodeStat
}

// NodeStat stores the information of a Node
type NodeStat struct {
	Address  string
	Status   string
	KeyCount int
}

// NodeStats is an array of NodeStat
type NodeStats []NodeStat

// NewSwimringClient returns a new SwimringClient instance.
func NewSwimringClient(address string, port int) *SwimringClient {
	c := &SwimringClient{
		address:    address,
		port:       port,
		readLevel:  ALL,
		writeLevel: ALL,
	}

	return c
}

// SetReadLevel sets the readLevel to specific level.
func (c *SwimringClient) SetReadLevel(level string) {
	c.readLevel = level
}

// SetWriteLevel sets the writeLevel to specific level.
func (c *SwimringClient) SetWriteLevel(level string) {
	c.writeLevel = level
}

// Connect establishes a connection to remote RPC server.
func (c *SwimringClient) Connect() error {
	var err error
	c.client, err = rpc.Dial("tcp", fmt.Sprintf("%s:%d", c.address, c.port))
	if err != nil {
		return err
	}

	return nil
}

// Get calls the remote Get method and returns the requested value.
func (c *SwimringClient) Get(key string) (string, error) {
	if c.client == nil {
		return "", errors.New("not connected")
	}

	req := &GetRequest{
		Key:   key,
		Level: c.readLevel,
	}
	resp := &GetResponse{}

	err := c.client.Call(GetOp, req, resp)
	if err != nil {
		return "", err
	}

	return resp.Value, nil
}

// Put calls the remote Put method to update for specific key.
func (c *SwimringClient) Put(key, value string) error {
	if c.client == nil {
		return errors.New("not connected")
	}

	req := &PutRequest{
		Key:   key,
		Value: value,
		Level: c.writeLevel,
	}
	resp := &PutResponse{}

	err := c.client.Call(PutOp, req, resp)
	if err != nil {
		return err
	}

	return nil
}

// Delete calls the remote Delete method to remove specific key.
func (c *SwimringClient) Delete(key string) error {
	if c.client == nil {
		return errors.New("not connected")
	}

	req := &DeleteRequest{
		Key:   key,
		Level: c.writeLevel,
	}
	resp := &DeleteResponse{}

	err := c.client.Call(DeleteOp, req, resp)
	if err != nil {
		return err
	}

	return nil
}

// Stat calls the remote Stat method to gather Nodes' information.
func (c *SwimringClient) Stat() (NodeStats, error) {
	if c.client == nil {
		return nil, errors.New("not connected")
	}

	req := &StateRequest{}
	resp := &StateResponse{}

	err := c.client.Call(StatOp, req, resp)
	if err != nil {
		return nil, err
	}

	return NodeStats(resp.Nodes), nil
}

func (ns NodeStats) Len() int {
	return len(ns)
}

func (ns NodeStats) Less(i, j int) bool {
	itokens := strings.Split(ns[i].Address, ":")
	jtokens := strings.Split(ns[j].Address, ":")

	if itokens[0] != jtokens[0] {
		return itokens[0] < jtokens[0]
	}

	iport, _ := strconv.Atoi(itokens[1])
	jport, _ := strconv.Atoi(jtokens[1])
	return iport < jport
}

func (ns NodeStats) Swap(i, j int) {
	ns[i], ns[j] = ns[j], ns[i]
}

var client *SwimringClient

func main() {
	var serverAddr string
	var serverPort int
	var readLevel, writeLevel string

	flag.StringVar(&serverAddr, "host", "127.0.0.1", "address of server node")
	flag.IntVar(&serverPort, "port", 7000, "port number of server node")
	flag.StringVar(&readLevel, "rl", QUORUM, "read consistency level")
	flag.StringVar(&writeLevel, "wl", QUORUM, "write consistency level")
	flag.Parse()

	client = NewSwimringClient(serverAddr, serverPort)
	client.SetReadLevel(readLevel)
	client.SetWriteLevel(writeLevel)

	err := client.Connect()
	if err != nil {
		fmt.Printf("error: unable to connect to %s:%d\n", serverAddr, serverPort)
		os.Exit(0)
	}
	fmt.Printf("connected to %s:%d\n", serverAddr, serverPort)

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		command, _ := reader.ReadString('\n')
		if err := processCommand(strings.Trim(command, " \r\n")); err != nil {
			fmt.Println(err.Error())
		}
	}
}

func processCommand(line string) error {
	tokens := util.SafeSplit(line)

	if len(tokens) == 0 {
		return nil
	}

	switch tokens[0] {
	case GetCmd:
		processGet(tokens)
	case PutCmd:
		processPut(tokens)
	case DeleteCmd:
		processDelete(tokens)
	case StatCmd:
		processStat(tokens)
	case ExitCmd:
		os.Exit(0)
	default:
		return errors.New("unknown command")
	}

	return nil
}

func processGet(tokens []string) {
	if len(tokens) != 2 {
		fmt.Println("usage: get <key>")
		return
	}

	val, err := client.Get(tokens[1])
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return
	}

	fmt.Println(val)
}

func processPut(tokens []string) {
	if len(tokens) != 3 {
		fmt.Println("usage: put <key> <value>")
		return
	}

	err := client.Put(tokens[1], tokens[2])
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return
	}

	fmt.Println("ok")
}

func processDelete(tokens []string) {
	if len(tokens) != 2 {
		fmt.Println("usage: del <key>")
		return
	}

	err := client.Delete(tokens[1])
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return
	}

	fmt.Println("ok")
}

func processStat(tokens []string) {
	nodes, err := client.Stat()
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return
	}

	var data [][]string
	sort.Sort(nodes)

	for _, node := range nodes {
		var n []string
		n = append(n, node.Address)
		n = append(n, node.Status)
		n = append(n, strconv.Itoa(node.KeyCount))
		data = append(data, n)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Address", "Status", "Key Count"})

	for _, d := range data {
		table.Append(d)
	}
	table.Render()
}
