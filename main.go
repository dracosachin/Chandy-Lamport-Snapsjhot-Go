package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
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
	process     *Process
	hosts       []string
	isInitiator bool    // Variable to check if the process is the token initiator
	sleepTime   float64 // Variable to store sleep time (t seconds)
	mu          sync.Mutex
)

func main() {
	if len(os.Args) < 4 || os.Args[1] != "-h" || os.Args[3] != "-t" {
		log.Fatal("Usage: program -h hostsfile.txt -t <sleep_time> [-x]")
	}

	hostsfile := os.Args[2]
	var err error

	// Parse the -t parameter for sleep time
	sleepTime, err = strconv.ParseFloat(os.Args[4], 64)
	if err != nil {
		log.Fatalf("Invalid sleep time: %v", err)
	}

	isInitiator = false

	// Check if the -x flag is present, which means this process is the token initiator
	if len(os.Args) == 6 && os.Args[5] == "-x" {
		isInitiator = true
	}

	// Read the hosts file and build the ring
	hosts, err = LoadHosts(hostsfile)
	if err != nil {
		log.Fatalf("Failed to load hosts from file: %v", err)
	}

	// Set up the process ID, predecessor, and successor based on the hostsfile
	procID := getProcessID(hosts)
	setupProcess(procID, hosts)

	// Print initial state of the process
	fmt.Printf("{proc_id: %d, state: %d, predecessor: %d, successor: %d}\n", process.ID, process.State, process.Predecessor, process.Successor)

	// Start TCP listener for token processing
	go listenForToken()

	// If this process is the token initiator, start the token passing
	if isInitiator {
		log.Printf("Process %d is the token initiator, waiting 5 seconds to ensure all processes are ready...", process.ID)
		time.Sleep(5 * time.Second) // Delay to ensure all processes are up
		sendToken(process.Successor)
	}

	// Keep the process running
	select {}
}

// LoadHosts loads the list of hosts from the hostsfile
func LoadHosts(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var hosts []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		hosts = append(hosts, scanner.Text())
	}
	return hosts, scanner.Err()
}

// getProcessID retrieves the current process ID based on the hostname
func getProcessID(hosts []string) int {
	hostname, _ := os.Hostname()
	for i, host := range hosts {
		if host == hostname {
			return i + 1
		}
	}
	log.Fatalf("Hostname %s not found in hostsfile", hostname)
	return -1
}

// setupProcess sets the process's ID, predecessor, and successor
func setupProcess(id int, hosts []string) {
	process = &Process{
		ID:          id,
		State:       0,
		Predecessor: getPredecessorID(id, len(hosts)),
		Successor:   getSuccessorID(id, len(hosts)),
		Host:        hosts[id-1],
	}
}

// getPredecessorID returns the predecessor ID in the ring
func getPredecessorID(id, total int) int {
	if id == 1 {
		return total
	}
	return id - 1
}

// getSuccessorID returns the successor ID in the ring
func getSuccessorID(id, total int) int {
	if id == total {
		return 1
	}
	return id + 1
}

// listenForToken listens for incoming token messages
func listenForToken() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting listener: %v", err)
	}
	defer listener.Close()

	// log.Printf("Process %d is listening for token on port 8080", process.ID)

	for {
		conn, err := listener.Accept()
		if err != nil {
			// log.Printf("Error accepting connection: %v", err)
			continue
		}
		// log.Printf("Process %d received a connection", process.ID)
		go handleToken(conn)
	}
}

// handleToken processes the received token
func handleToken(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("Error reading token: %v", err)
		return
	}

	token := string(buf[:n])
	fmt.Printf("{proc_id: %d, sender: %s, receiver: %d, message:\"token\"}\n", process.ID, token, process.ID)

	// Increment state
	mu.Lock()
	process.State++
	fmt.Printf("{proc_id: %d, state: %d}\n", process.ID, process.State)
	mu.Unlock()

	// Sleep for t seconds as provided in the -t parameter
	time.Sleep(time.Duration(sleepTime * float64(time.Second)))

	// Pass the token to the successor in a separate goroutine
	go sendToken(process.Successor)
}

// sendToken sends the token to the successor
func sendToken(successorID int) {
	successorHost := hosts[successorID-1]
	var conn net.Conn
	var err error

	// Retry connection to successor with a delay
	for retries := 0; retries < 5; retries++ {
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:8080", successorHost))
		if err != nil {
			log.Printf("Error connecting to successor %d: %v, retrying...", successorID, err)
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}

	if err != nil {
		log.Printf("Failed to connect to successor %d after retries", successorID)
		return
	}
	defer conn.Close()

	// Send the token to the successor
	_, err = conn.Write([]byte(fmt.Sprintf("proc_id: %d", process.ID)))
	if err != nil {
		log.Printf("Error sending token: %v", err)
	}
	fmt.Printf("{proc_id: %d, sender: %d, receiver: %d, message:\"token\"}\n", process.ID, process.ID, successorID)
}
