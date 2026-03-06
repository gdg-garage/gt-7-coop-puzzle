package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"math/rand"
	"os"
	"strconv"

	"github.com/gorilla/websocket"
)

var (
	cipherText   = "THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG"
	cipherKey    = ""
	minClients   = 4
	useSessionID = false
)

func init() {
	if val, ok := os.LookupEnv("MIN_CLIENTS"); ok {
		if n, err := strconv.Atoi(val); err == nil {
			minClients = n
		}
	}
	if val, ok := os.LookupEnv("USE_SESSION_ID"); ok {
		useSessionID = val == "true"
	}
	if val, ok := os.LookupEnv("CIPHER_TEXT"); ok {
		cipherText = val
	}
	if val, ok := os.LookupEnv("CIPHER_KEY"); ok {
		cipherKey = val
	} else {
		cipherKey = generateRandomKey(16)
	}

	state = &ServerState{
		ConnectedIDs: make(map[string]int),
		JoinedIDs:    make(map[string]bool),
		IDToChunk:    make(map[string]int),
		Chunks:       make([]Chunk, minClients),
	}
	for i := 0; i < minClients; i++ {
		state.Chunks[i] = Chunk{ID: i + 1, Status: "gray", Timer: 0}
	}
}

func generateRandomKey(length int) string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func caesarCipher(text, key string) string {
	var result strings.Builder
	keyLen := len(key)
	for i, char := range text {
		if char >= 'A' && char <= 'Z' {
			shift := int(key[i%keyLen] - 'A')
			newChar := 'A' + (char-'A'+rune(shift))%26
			result.WriteRune(newChar)
		} else if char >= 'a' && char <= 'z' {
			shift := int(strings.ToUpper(string(key[i%keyLen]))[0] - 'A')
			newChar := 'a' + (char-'a'+rune(shift))%26
			result.WriteRune(newChar)
		} else {
			result.WriteRune(char)
		}
	}
	return result.String()
}

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
	ConnectedIDs map[string]int  `json:"connected_ids"`
	JoinedIDs    map[string]bool `json:"joined_ids"`
	IDToChunk    map[string]int  `json:"-"` // Track which chunk each client joined
	mu           sync.Mutex
}

var state *ServerState

var clients = make(map[*websocket.Conn]string)
var clientsMu sync.Mutex

func getClientID(r *http.Request) string {
	if useSessionID {
		return fmt.Sprintf("session-%d-%d", time.Now().UnixNano(), rand.Intn(1000000))
	}

	// Check X-Forwarded-For (Traefik/proxies)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		ips := strings.Split(xff, ",")
		return strings.TrimSpace(ips[0])
	}

	// Check X-Real-IP
	if xip := r.Header.Get("X-Real-IP"); xip != "" {
		return xip
	}

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

	uniqueCount := len(state.ConnectedIDs)
	targetChunks := uniqueCount
	if targetChunks < minClients {
		targetChunks = minClients
	}

	// Add new chunks if needed
	for len(state.Chunks) < targetChunks {
		newID := len(state.Chunks) + 1
		state.Chunks = append(state.Chunks, Chunk{ID: newID, Status: "gray", Timer: 0})
	}

	// Remove excess chunks from the end, but only non-green ones
	for len(state.Chunks) > targetChunks {
		last := state.Chunks[len(state.Chunks)-1]
		if last.Status == "green" {
			break // don't remove a chunk that is actively joined
		}
		state.Chunks = state.Chunks[:len(state.Chunks)-1]
	}

	for i := range state.Chunks {
		if state.Chunks[i].Status == "green" {
			continue
		}

		if i < uniqueCount {
			state.Chunks[i].Status = "orange"
		} else {
			state.Chunks[i].Status = "gray"
		}
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	id := getClientID(r)
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS upgrade error: %v", err)
		return
	}
	defer ws.Close()

	state.mu.Lock()
	state.ConnectedIDs[id]++
	totalConnected := len(state.ConnectedIDs)
	state.mu.Unlock()

	log.Printf("client connected: %s (total connected: %d)", id, totalConnected)

	updateStatuses()

	clientsMu.Lock()
	clients[ws] = id
	clientsMu.Unlock()

	// Send initial state
	state.mu.Lock()
	var cipherChunk string
	if state.JoinedIDs[id] {
		// If already joined and potentially unlocked, re-distribute or re-send cipher part
		cipherChunk = getCipherForID(id)
	}

	initialPayload, _ := json.Marshal(map[string]interface{}{
		"chunks":         state.Chunks,
		"already_joined": state.JoinedIDs[id],
		"cipher_chunk":   cipherChunk,
	})
	state.mu.Unlock()
	ws.WriteMessage(websocket.TextMessage, initialPayload)

	broadcast(map[string]interface{}{"chunks": state.Chunks})

	defer func() {
		state.mu.Lock()
		state.ConnectedIDs[id]--
		if state.ConnectedIDs[id] <= 0 {
			delete(state.ConnectedIDs, id)
			delete(state.JoinedIDs, id)
			delete(state.IDToChunk, id)
		}
		totalRemaining := len(state.ConnectedIDs)
		state.mu.Unlock()
		updateStatuses()
		broadcast(map[string]interface{}{"chunks": state.Chunks})

		clientsMu.Lock()
		delete(clients, ws)
		clientsMu.Unlock()
		log.Printf("client disconnected: %s (total connected: %d)", id, totalRemaining)
	}()

	for {
		var msg map[string]string
		err := ws.ReadJSON(&msg)
		if err != nil {
			break
		}

		if action, ok := msg["action"]; ok && action == "join" {
			state.mu.Lock()
			if state.JoinedIDs[id] {
				ws.WriteJSON(map[string]interface{}{
					"error":          "you are already joined!",
					"already_joined": true,
				})
				state.mu.Unlock()
				continue
			}

			greenCount := 0
			for _, c := range state.Chunks {
				if c.Status == "green" {
					greenCount++
				}
			}

			if greenCount >= len(state.Chunks) {
				ws.WriteJSON(map[string]string{"error": "enough people already joined"})
				state.mu.Unlock()
				continue
			}

			var firstOrange *Chunk
			for i := range state.Chunks {
				if state.Chunks[i].Status == "orange" {
					firstOrange = &state.Chunks[i]
					break
				}
			}

			if firstOrange != nil {
				firstOrange.Status = "green"
				firstOrange.Timer = 20
				state.JoinedIDs[id] = true
				state.IDToChunk[id] = firstOrange.ID
				joinedCount := len(state.JoinedIDs)

				log.Printf("client joined: %s (chunk: %d, total joined: %d/%d)", id, firstOrange.ID, joinedCount, len(state.Chunks))

				broadcast(map[string]interface{}{"chunks": state.Chunks})
				ws.WriteJSON(map[string]interface{}{"already_joined": true})

				// Check for unlock - at least 4 green chunks
				newGreenCount := 0
				for _, c := range state.Chunks {
					if c.Status == "green" {
						newGreenCount++
					}
				}

				if newGreenCount >= minClients {
					broadcast(map[string]bool{"unlocked": true})
					distributeCipher()
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
					// Find and clear the client that joined this chunk
					for id, chunkID := range state.IDToChunk {
						if chunkID == state.Chunks[i].ID {
							delete(state.JoinedIDs, id)
							delete(state.IDToChunk, id)
							// Notify this specific client they can rejoin
							clientsMu.Lock()
							for client, clientID := range clients {
								if clientID == id {
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

func getCipherForID(id string) string {
	// Re-calculate the cipher chunk for a specific client if they are already joined
	joinedIDs := []string{}
	for joinedID := range state.JoinedIDs {
		joinedIDs = append(joinedIDs, joinedID)
	}

	n := len(joinedIDs)
	if n < minClients {
		return ""
	}

	numKeyChunks := n / 2
	numCipherChunks := n - numKeyChunks

	encrypted := caesarCipher(cipherText, cipherKey)
	cipherParts := splitText(encrypted, numCipherChunks)
	keyParts := splitText(cipherKey, numKeyChunks)

	for i, joinedID := range joinedIDs {
		if joinedID == id {
			if i < numCipherChunks {
				return fmt.Sprintf("c_%d: %s", i, cipherParts[i])
			} else {
				keyIdx := i - numCipherChunks
				return fmt.Sprintf("k_%d: %s", keyIdx, keyParts[keyIdx])
			}
		}
	}
	return ""
}

func distributeCipher() {
	// Already under state.mu lock from handleConnections
	joinedIDs := []string{}
	for id := range state.JoinedIDs {
		joinedIDs = append(joinedIDs, id)
	}

	n := len(joinedIDs)
	if n == 0 {
		return
	}

	numKeyChunks := n / 2
	numCipherChunks := n - numKeyChunks

	encrypted := caesarCipher(cipherText, cipherKey)

	cipherParts := splitText(encrypted, numCipherChunks)
	keyParts := splitText(cipherKey, numKeyChunks)

	// Send to clients
	clientsMu.Lock()
	defer clientsMu.Unlock()

	cipherIdx := 0
	keyIdx := 0

	for i, id := range joinedIDs {
		var msg string
		if i < numCipherChunks {
			msg = fmt.Sprintf("c_%d: %s", cipherIdx, cipherParts[cipherIdx])
			cipherIdx++
		} else {
			msg = fmt.Sprintf("k_%d: %s", keyIdx, keyParts[keyIdx])
			keyIdx++
		}

		for client, clientID := range clients {
			if clientID == id {
				log.Printf("distributing cipher to %s: %s", id, msg)
				client.WriteJSON(map[string]string{"cipher_chunk": msg})
			}
		}
	}
}

func splitText(text string, parts int) []string {
	if parts <= 0 {
		return nil
	}
	n := len(text)
	result := make([]string, parts)
	if n == 0 {
		return result
	}

	chunkSize := n / parts
	remainder := n % parts

	start := 0
	for i := 0; i < parts; i++ {
		end := start + chunkSize
		if i < remainder {
			end++
		}
		if end > n {
			end = n
		}
		result[i] = text[start:end]
		start = end
	}
	return result
}

func main() {
	rand.Seed(time.Now().UnixNano())
	go tickerLoop()

	http.HandleFunc("/ws", handleConnections)
	http.Handle("/", http.FileServer(http.Dir(".")))

	fmt.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
