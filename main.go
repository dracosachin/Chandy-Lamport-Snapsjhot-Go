package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// LoadHosts loads the list of other peers from the hostsfile
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

// ListenForPeers listens for incoming TCP connections and signals readiness
func ListenForPeers(port string, wg *sync.WaitGroup, peers *map[string]bool, mu *sync.Mutex, totalPeers int) {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Error starting TCP listener: ", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()

			buf := make([]byte, 1024)
			n, err := c.Read(buf)
			if err != nil {
				log.Println("Error reading from connection: ", err)
				return
			}
			peer := string(buf[:n])
			log.Printf("Received message from %s", peer)

			mu.Lock()
			if _, ok := (*peers)[peer]; !ok {
				(*peers)[peer] = true
			}
			mu.Unlock()

			// Check if we received messages from all peers (n-1)
			mu.Lock()
			if len(*peers) == totalPeers-1 {
				fmt.Fprintln(os.Stderr, "READY")
				wg.Done()
			}
			mu.Unlock()

		}(conn)
	}
}

// NotifyPeers sends a heartbeat to all other peers
func NotifyPeers(hosts []string, hostname string) {
	for _, host := range hosts {
		if host == hostname {
			continue
		}

		for {
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:8080", host))
			if err != nil {
				log.Printf("Error connecting to %s, retrying in 2 seconds...", host)
				time.Sleep(2 * time.Second)
				continue
			}
			conn.Write([]byte(hostname))
			conn.Close()
			break
		}
	}
}

func main() {
	if len(os.Args) < 3 || os.Args[1] != "-h" {
		log.Fatal("Usage: program -h hostsfile.txt")
	}

	hostname, _ := os.Hostname()
	hostsfile := os.Args[2]

	hosts, err := LoadHosts(hostsfile)
	if err != nil {
		log.Fatalf("Failed to load hosts from file: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	peers := make(map[string]bool)
	var mu sync.Mutex

	totalPeers := len(hosts)

	go ListenForPeers(":8080", &wg, &peers, &mu, totalPeers)

	// Wait a moment before notifying peers to give others time to set up
	time.Sleep(2 * time.Second)

	// Notify other peers
	NotifyPeers(hosts, hostname)

	// Wait until we've received messages from all other peers
	// wg.Wait()
}
