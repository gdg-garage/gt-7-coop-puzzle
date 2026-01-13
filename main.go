package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for this project
	},
}

type Chunk struct {
	ID     int    `json:"id"`
	Status string `json:"status"` // "gray" | "orange" | "green"
	Timer  int    `json:"timer"`
}

type ServerState struct {
	Chunks       []Chunk         `json:"chunks"`
	ConnectedIPs map[string]int  `json:"connected_ips"`
	JoinedIPs    map[string]bool `json:"joined_ips"`
	IPToChunk    map[string]int  `json:"-"` // Track which chunk each IP joined
	mu           sync.Mutex
}

var state = &ServerState{
	Chunks: []Chunk{
		{ID: 1, Status: "gray", Timer: 0},
		{ID: 2, Status: "gray", Timer: 0},
		{ID: 3, Status: "gray", Timer: 0},
		{ID: 4, Status: "gray", Timer: 0},
	},
	ConnectedIPs: make(map[string]int),
	JoinedIPs:    make(map[string]bool),
	IPToChunk:    make(map[string]int),
}

var clients = make(map[*websocket.Conn]string)
var clientsMu sync.Mutex

func getIP(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

func broadcast(msg interface{}) {
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("error marshaling: %v", err)
		return
	}

	clientsMu.Lock()
	defer clientsMu.Unlock()
	for client, ip := range clients {
		err := client.WriteMessage(websocket.TextMessage, payload)
		if err != nil {
			log.Printf("error broadcasting to client %s: %v", ip, err)
			client.Close()
			delete(clients, client)
		}
	}
}

func updateStatuses() {
	state.mu.Lock()
	defer state.mu.Unlock()

	uniqueCount := len(state.ConnectedIPs)
	greenCount := 0
	for _, c := range state.Chunks {
		if c.Status == "green" {
			greenCount++
		}
	}

	for i := range state.Chunks {
		if state.Chunks[i].Status == "green" {
			continue
		}

		if uniqueCount >= 4 {
			state.Chunks[i].Status = "orange"
		} else if i < uniqueCount {
			state.Chunks[i].Status = "orange"
		} else {
			state.Chunks[i].Status = "gray"
		}
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS upgrade error: %v", err)
		return
	}
	defer ws.Close()

	ip := getIP(r)
	log.Printf("client connected: %s", ip)

	state.mu.Lock()
	state.ConnectedIPs[ip]++
	state.mu.Unlock()

	updateStatuses()

	clientsMu.Lock()
	clients[ws] = ip
	clientsMu.Unlock()

	// Send initial state
	state.mu.Lock()
	initialPayload, _ := json.Marshal(map[string]interface{}{
		"chunks":         state.Chunks,
		"already_joined": state.JoinedIPs[ip],
	})
	state.mu.Unlock()
	ws.WriteMessage(websocket.TextMessage, initialPayload)

	broadcast(map[string]interface{}{"chunks": state.Chunks})

	defer func() {
		state.mu.Lock()
		state.ConnectedIPs[ip]--
		if state.ConnectedIPs[ip] <= 0 {
			delete(state.ConnectedIPs, ip)
		}
		state.mu.Unlock()
		updateStatuses()
		broadcast(map[string]interface{}{"chunks": state.Chunks})

		clientsMu.Lock()
		delete(clients, ws)
		clientsMu.Unlock()
		log.Printf("client disconnected: %s", ip)
	}()

	for {
		var msg map[string]string
		err := ws.ReadJSON(&msg)
		if err != nil {
			break
		}

		if action, ok := msg["action"]; ok && action == "join" {
			state.mu.Lock()
			if state.JoinedIPs[ip] {
				ws.WriteJSON(map[string]interface{}{
					"error":          "you already joined.",
					"already_joined": true,
				})
				state.mu.Unlock()
				continue
			}

			greenCount := 0
			var firstOrange *Chunk
			for i := range state.Chunks {
				if state.Chunks[i].Status == "green" {
					greenCount++
				} else if state.Chunks[i].Status == "orange" && firstOrange == nil {
					firstOrange = &state.Chunks[i]
				}
			}

			if greenCount >= 4 {
				ws.WriteJSON(map[string]string{"error": "enough people already joined"})
				state.mu.Unlock()
				continue
			}

			if firstOrange != nil {
				firstOrange.Status = "green"
				firstOrange.Timer = 20
				state.JoinedIPs[ip] = true
				state.IPToChunk[ip] = firstOrange.ID

				// Check for unlock
				newGreenCount := 0
				for _, c := range state.Chunks {
					if c.Status == "green" {
						newGreenCount++
					}
				}

				broadcast(map[string]interface{}{"chunks": state.Chunks})
				ws.WriteJSON(map[string]interface{}{"already_joined": true})

				if newGreenCount == 4 {
					broadcast(map[string]bool{"unlocked": true})
				}
			}
			state.mu.Unlock()
		}
	}
}

func tickerLoop() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		state.mu.Lock()
		updated := false
		for i := range state.Chunks {
			if state.Chunks[i].Timer > 0 {
				state.Chunks[i].Timer--
				if state.Chunks[i].Timer == 0 {
					state.Chunks[i].Status = "gray" // Will be recalculated by updateStatuses
					// Find and clear the IP that joined this chunk
					for ip, chunkID := range state.IPToChunk {
						if chunkID == state.Chunks[i].ID {
							delete(state.JoinedIPs, ip)
							delete(state.IPToChunk, ip)
							// Notify this specific IP they can rejoin
							clientsMu.Lock()
							for client, clientIP := range clients {
								if clientIP == ip {
									client.WriteJSON(map[string]bool{"can_rejoin": true})
								}
							}
							clientsMu.Unlock()
							break
						}
					}
				}
				updated = true
			}
		}
		state.mu.Unlock()

		if updated {
			updateStatuses()
			broadcast(map[string]interface{}{"chunks": state.Chunks})
		}
	}
}

func main() {
	go tickerLoop()

	http.HandleFunc("/ws", handleConnections)
	http.Handle("/", http.FileServer(http.Dir(".")))

	fmt.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
