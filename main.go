package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Process struct {
	ID          int
	State       int
	Predecessor int
	Successor   int
	Host        string
}

var (
	process           *Process
	hosts             []string
	sleepTime         float64 // Sleep time in seconds (-t parameter)
	markerDelay       float64 // Delay between receiving and sending markers (-m parameter)
	initiateState     int     // State at which to initiate a snapshot (-s parameter)
	snapshotID        int     // Snapshot identifier (-p parameter)
	mu                sync.Mutex
	incomingQueues    map[string][]string // Incoming message queues per channel
	channelState      map[string]bool     // Tracks whether a channel is still open (true) or closed (false)
	recording         bool                // Indicates if the process is recording messages
	snapshotState     int                 // Process state when snapshot started
	hasToken          bool                // Indicates if the process currently has the token
	snapshotInitiator bool                // Indicates if this process will initiate a snapshot
)

func main() {

	hostsfile := os.Args[2]
	var err error

	if len(os.Args) < 7 {
		sleepTime, err = strconv.ParseFloat(os.Args[4], 64)
		if err != nil {
			log.Fatalf("Invalid sleep time: %v", err)
		}
	} else {
		sleepTime, err = strconv.ParseFloat(os.Args[6], 64)
		if err != nil {
			log.Fatalf("Invalid sleep time: %v", err)
		}
	}

	markerDelay, err = strconv.ParseFloat(os.Args[4], 64)
	if err != nil {
		log.Fatalf("Invalid marker delay: %v", err)
	}

	initiateState = -1
	snapshotID = -1

	snapshotInitiator = false
	for i := 7; i < len(os.Args); i++ {
		if os.Args[i] == "-s" && i+1 < len(os.Args) {
			initiateState, err = strconv.Atoi(os.Args[i+1])
			if err != nil {
				log.Fatalf("Invalid snapshot state: %v", err)
			}
			snapshotInitiator = true
			// fmt.Printf("Process will start snapshot at state %d\n", initiateState)
		}
		if os.Args[i] == "-p" && i+1 < len(os.Args) {
			snapshotID, err = strconv.Atoi(os.Args[i+1])
			if err != nil {
				log.Fatalf("Invalid snapshot ID: %v", err)
			}
			// fmt.Printf("Snapshot ID set to %d\n", snapshotID)
		}
	}

	hosts, err = LoadHosts(hostsfile)
	if err != nil {
		log.Fatalf("Failed to load hosts from file: %v", err)
	}

	setupProcess(getProcessID(hosts), hosts)

	incomingQueues = make(map[string][]string)
	channelState = make(map[string]bool)

	fmt.Printf("{proc_id: %d, state: %d, predecessor: %d, successor: %d}\n", process.ID, process.State, process.Predecessor, process.Successor)

	go listenForMessages()

	isInitiator := false
	if os.Args[len(os.Args)-1] == "-x" {
		isInitiator = true
	}

	if isInitiator {
		// log.Printf("Process %d is the token initiator, waiting 5 seconds to ensure all processes are ready...", process.ID)
		time.Sleep(5 * time.Second)
		sendMessage(process.Successor, "token")
	}

	if snapshotInitiator && initiateState > 0 {
		go func() {
			for {
				if process.State == initiateState && !recording {
					if snapshotID == -1 {
						snapshotID = int(time.Now().UnixNano() & 0x7FFFFFFF)
						// fmt.Printf("Generated snapshot ID: %d\n", snapshotID)
					}
					startSnapshot(markerDelay)
				}
				time.Sleep(1 * time.Second)
			}
		}()
	}

	select {}
}

func LoadHosts(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var hosts []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			hosts = append(hosts, line)
		}
	}
	return hosts, scanner.Err()
}

func getProcessID(hosts []string) int {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Error retrieving hostname: %v", err)
	}

	for i, host := range hosts {
		if host == hostname {
			return i + 1
		}
	}
	log.Fatalf("Hostname %s not found in hosts file", hostname)
	return -1
}

func setupProcess(id int, hosts []string) {
	total := len(hosts)
	process = &Process{
		ID:          id,
		State:       0,
		Predecessor: getPredecessorID(id, total),
		Successor:   getSuccessorID(id, total),
		Host:        hosts[id-1],
	}
}

func getPredecessorID(id, total int) int {
	if id == 1 {
		return total
	}
	return id - 1
}

func getSuccessorID(id, total int) int {
	if id == total {
		return 1
	}
	return id + 1
}

func listenForMessages() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting listener: %v", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handleMessage(conn)
	}
}

func handleMessage(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("Error reading message: %v", err)
		return
	}

	message := string(buf[:n])

	if strings.Contains(message, "marker") {
		handleMarker(message)
		return
	}

	if strings.Contains(message, "token") {
		handleToken(message)
		return
	}

	if recording {
		parts := strings.Split(message, ",")
		senderField := parts[1] // Assuming format "proc_id:X"
		senderIDStr := extractField(senderField, "proc_id")
		senderID, err := strconv.Atoi(senderIDStr)
		if err != nil {
			log.Printf("Error parsing sender ID from message: %v", err)
			return
		}
		channel := fmt.Sprintf("%d->%d", senderID, process.ID)
		if channelState[channel] { // If this channel is still open, record the message
			incomingQueues[channel] = append(incomingQueues[channel], message)
		}
	}
}

func handleToken(message string) {
	parts := strings.Split(message, ",")
	senderField := parts[1] // Assuming format "proc_id:X"
	senderIDStr := extractField(senderField, "proc_id")
	senderID, err := strconv.Atoi(senderIDStr)
	if err != nil {
		log.Printf("Error parsing sender ID from token message: %v", err)
		return
	}

	mu.Lock()
	process.State++
	hasToken = true
	fmt.Printf("{proc_id: %d, state: %d}\n", process.ID, process.State)
	mu.Unlock()

	if recording {
		channel := fmt.Sprintf("%d->%d", senderID, process.ID)
		if channelState[channel] {
			incomingQueues[channel] = append(incomingQueues[channel], message)
		}
	}

	time.Sleep(time.Duration(sleepTime * float64(time.Second)))

	sendMessage(process.Successor, "token")
}

func sendMessage(receiverID int, msgType string) {
	receiverHost := hosts[receiverID-1]
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:8080", receiverHost))
	if err != nil {
		log.Printf("Error connecting to process %d: %v", receiverID, err)
		return
	}
	defer conn.Close()

	var message string
	if msgType == "token" {
		message = fmt.Sprintf("token,proc_id:%d", process.ID)
		fmt.Printf("{proc_id: %d, sender: %d, receiver: %d, message:\"token\"}\n", process.ID, process.ID, receiverID)
	}

	_, err = conn.Write([]byte(message))
	if err != nil {
		log.Printf("Error sending %s to process %d: %v", msgType, receiverID, err)
	}
}

func startSnapshot(markerDelay float64) {
	fmt.Printf("{proc_id: %d, snapshot_id: %d, snapshot:\"started\"}\n", process.ID, snapshotID)
	snapshotState = process.State

	recording = true

	for _, host := range hosts {
		if host != process.Host {
			peerID := getProcessIDFromHost(host)
			channel := fmt.Sprintf("%d->%d", peerID, process.ID)
			channelState[channel] = true
			incomingQueues[channel] = []string{}
			// fmt.Printf("Opening channel: %s\n", channel) // Debug print
		}
	}

	for _, host := range hosts {
		if host != process.Host {
			go func(host string) {
				time.Sleep(time.Duration(markerDelay * float64(time.Second)))
				sendMessageToHost(host, "marker")
			}(host)
		}
	}
}

func sendMessageToHost(host string, msgType string) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:8080", host))
	if err != nil {
		log.Printf("Error connecting to %s to send %s: %v", host, msgType, err)
		return
	}
	defer conn.Close()

	var message string
	if msgType == "marker" {
		message = fmt.Sprintf("marker,proc_id:%d,snapshot_id:%d,state:%d", process.ID, snapshotID, process.State)
		receiverID := getProcessIDFromHost(host)
		fmt.Printf("{proc_id: %d, snapshot_id: %d, sender: %d, receiver: %d, msg:\"marker\", state: %d, has_token: %t}\n",
			process.ID, snapshotID, process.ID, receiverID, process.State, hasToken)
	}

	_, err = conn.Write([]byte(message))
	if err != nil {
		log.Printf("Error sending %s to host %s: %v", msgType, host, err)
	}
}

func handleMarker(message string) {
	parts := strings.Split(message, ",")
	if len(parts) < 3 {
		log.Printf("Invalid marker message: %s", message)
		return
	}

	senderIDStr := extractField(parts[1], "proc_id")
	senderID, err := strconv.Atoi(senderIDStr)
	if err != nil {
		log.Printf("Error parsing sender ID from marker message: %v", err)
		return
	}
	receivedSnapshotID := extractField(parts[2], "snapshot_id")
	receivedSnapshotIDInt, err := strconv.Atoi(receivedSnapshotID)
	if err != nil {
		log.Printf("Error parsing snapshot ID from marker message: %v", err)
		return
	}

	if snapshotID == -1 {
		snapshotID = receivedSnapshotIDInt
		fmt.Printf("Process %d received snapshot ID: %d\n", process.ID, snapshotID)
	} else if snapshotID != receivedSnapshotIDInt {
		fmt.Printf("Process %d received marker for snapshot ID %d, but already participating in snapshot ID %d\n",
			process.ID, receivedSnapshotIDInt, snapshotID)
		return
	}

	channel := fmt.Sprintf("%d->%d", senderID, process.ID)

	if !recording {
		recording = true
		snapshotState = process.State
		fmt.Printf("{proc_id: %d, snapshot_id: %d, snapshot:\"started\"}\n", process.ID, snapshotID)

		// Initialize all channels to open (except for the channel on which the marker was received)
		for _, host := range hosts {
			if host != process.Host {
				peerID := getProcessIDFromHost(host)
				otherChannel := fmt.Sprintf("%d->%d", peerID, process.ID)
				if otherChannel != channel {
					channelState[otherChannel] = true
					incomingQueues[otherChannel] = []string{}
					// fmt.Printf("Opening channel: %s\n", otherChannel) // Debug print
				}
			}
		}

		for _, host := range hosts {
			if host != process.Host {
				go func(host string) {
					time.Sleep(time.Duration(markerDelay * float64(time.Second)))
					sendMessageToHost(host, "marker")
				}(host)
			}
		}
	}

	// fmt.Printf("Closing channel: %s\n", channel) // Debug print
	channelState[channel] = false
	fmt.Printf("{proc_id: %d, snapshot_id: %d, snapshot:\"channel closed\", channel: %s, queue: [%s]}\n",
		process.ID, snapshotID, channel, strings.Join(incomingQueues[channel], ","))
	incomingQueues[channel] = nil

	checkSnapshotCompletion()
}

func checkSnapshotCompletion() {
	allClosed := true

	// fmt.Println("Checking if all channels are closed:")
	for _, open := range channelState {
		// fmt.Printf("Channel %s is %v\n", channel, open)
		if open {
			allClosed = false
		}
	}

	if allClosed {
		completeSnapshot()
	} else {
		// fmt.Println("Not all channels are closed yet.") // Debug print
	}
}

// completeSnapshot completes the Chandy-Lamport snapshot
func completeSnapshot() {
	fmt.Printf("{proc_id: %d, snapshot_id: %d, snapshot:\"complete\"}\n", process.ID, snapshotID)
	recording = false
}

func extractField(s, key string) string {
	parts := strings.Split(s, ":")
	if len(parts) == 2 && parts[0] == key {
		return parts[1]
	}
	return ""
}

func getProcessIDFromHost(host string) int {
	for i, h := range hosts {
		if h == host {
			return i + 1
		}
	}
	return -1
}
