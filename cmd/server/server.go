// cmd/server/server.go
package server

import (
	"Lora_Esp_Gsm_Gps_project/internal/core"
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	conn            net.Conn
	device          string
	lastInteraction string
}

type Server struct {
	clients            map[string]*Client
	mutex              sync.RWMutex
	measurementHandler core.MeasurementHandler
}

func NewServer(h core.MeasurementHandler) *Server {
	return &Server{
		clients:            make(map[string]*Client),
		measurementHandler: h,
	}
}

func (s *Server) StartServer(port string) error {

	log.Printf("Starting server on port %v", port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))

	if err != nil {
		log.Fatalln(fmt.Errorf("error starting server: %v", err))
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}(listener)
	defer s.deleteAllClients()
	log.Printf("Server started listening on port %v", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalln(fmt.Errorf("error accepting connection: %v", err.Error()))
		}
		log.Printf("Accepted connection from %v", conn.RemoteAddr())
		go func() {
			err := s.handleConnection(conn)
			if err != nil {
				log.Printf("Error handling connection: %v", err)
			}
		}()
	}
}

func (s *Server) handleConnection(conn net.Conn) error {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)
	buffer := make([]byte, 1024)
	scanner := bufio.NewScanner(conn)
	count := 0
	for scanner.Scan() {
		message := scanner.Text()
		buffer = append(buffer, message...)
		if strings.Contains(message, "request_id") {
			go func() {
				err := s.measurementHandler.ProcessPacketData(message)
				if err != nil {
					log.Printf("Error processing packet data: %v", err)
				}
			}()
			s.registerClient("Master"+strconv.Itoa(count), conn)
		} else {
			s.registerClient("interface"+strconv.Itoa(count), conn)
			go func() {
				err := s.measurementHandler.ProcessInterfaceRequest(message)
				if err != nil {
					log.Printf("Error processing interface request: %v", err)
				}
			}()
		}
		count++
		log.Printf("Received message: %v", message)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading:", err.Error())
		return err
	}
	return nil
}

func (s *Server) registerClient(deviceID string, conn net.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.clients[deviceID] = &Client{
		conn:            conn,
		lastInteraction: time.Now().String(),
	}

	log.Printf("Registered client: %s", deviceID)
}

func (s *Server) unregisterClient(deviceID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if client, exists := s.clients[deviceID]; exists {
		err := client.conn.Close()
		if err != nil {
			log.Printf("Error closing connection for client %s: %v", deviceID, err)
			return
		}
		delete(s.clients, deviceID)
		log.Printf("Unregistered client: %s", deviceID)
	}
}

func (s *Server) deleteAllClients() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, client := range s.clients {
		s.unregisterClient(client.device)
	}
}
