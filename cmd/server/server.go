package server

import (
	"Lora_Esp_Gsm_Gps_project/cmd/app"
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var appInstance *app.App

func Init(app *app.App) {
	appInstance = app
}

type Client struct {
	conn            net.Conn
	device          string
	lastInteraction string
}

type Server struct {
	clients map[string]*Client
	mutex   sync.RWMutex
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
	defer deleteAllClients()
	log.Printf("Server started listening on port %v", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalln(fmt.Errorf("error accepting connection: %v", err.Error()))
		}
		log.Printf("Accepted connection from %v", conn.RemoteAddr())
		go func() {
			err := handleConnection(conn)
			if err != nil {
				log.Printf("Error handling connection: %v", err)
			}
		}()
	}
}

func handleConnection(conn net.Conn) error {
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
				err := appInstance.DataService.ProcessPacketData(message)
				if err != nil {
					log.Printf("Error processing packet data: %v", err)
				}
			}()
			registerClient("Master"+strconv.Itoa(count), conn)
		} else {
			registerClient("interface"+strconv.Itoa(count), conn)
			go func() {
				err := appInstance.DataService.ProcessInterfaceRequest(message)
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

func registerClient(deviceID string, conn net.Conn) {
	appInstance.Server.mutex.Lock()
	defer appInstance.Server.mutex.Unlock()

	appInstance.Server.clients[deviceID] = &Client{
		conn:            conn,
		lastInteraction: time.Now().String(),
	}

	log.Printf("Registered client: %s", deviceID)
}

func unregisterClient(deviceID string) {
	appInstance.Server.mutex.Lock()
	defer appInstance.Server.mutex.Unlock()

	if client, exists := appInstance.Server.clients[deviceID]; exists {
		err := client.conn.Close()
		if err != nil {
			log.Printf("Error closing connection for client %s: %v", deviceID, err)
			return
		}
		delete(appInstance.Server.clients, deviceID)
		log.Printf("Unregistered client: %s", deviceID)
	}
}

func deleteAllClients() {
	appInstance.Server.mutex.Lock()
	defer appInstance.Server.mutex.Unlock()
	for _, client := range appInstance.Server.clients {
		unregisterClient(client.device)
	}
}
