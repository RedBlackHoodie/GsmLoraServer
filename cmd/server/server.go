package server

import (
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"Lora_Esp_Gsm_Gps_project/internal/service"
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var cfg = configs.LoadConfig()
var dataService = service.DataService{}

type Client struct {
	conn            net.Conn
	device          string
	lastInteraction string
}

type Server struct {
	clients map[string]*Client
	mutex   sync.RWMutex
}

var ServerInst = &Server{
	clients: make(map[string]*Client),
}

func StartServer() error {
	port := cfg.Port

	log.Printf("Starting server on port %v", port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))

	if err != nil {
		log.Fatalln(fmt.Errorf("error starting server: %v", err))
	}
	defer listener.Close()
	defer deleteAllClients()
	log.Printf("Server started listening on port %v", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalln(fmt.Errorf("error accepting connection: %v", err.Error()))
		}
		log.Printf("Accepted connection from %v", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) error {
	defer conn.Close()
	buffer := make([]byte, 1024)
	scanner := bufio.NewScanner(conn)
	count := 0
	for scanner.Scan() {
		message := scanner.Text()
		buffer = append(buffer, message...)
		if strings.Contains(message, "request_id") {
			go dataService.ProcessPacketData(message)
			registerClient("Master"+strconv.Itoa(count), conn)
		} else {
			registerClient("interface"+strconv.Itoa(count), conn)
			go dataService.ProcessInterfaceRequest(message)
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

func SendParamsToDevice(ip string, port string, config models.Params) {
	target := ip + ":" + port
	timeout := 10 * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		log.Fatalln(fmt.Errorf("error connecting to device: %v", err))
	}
	defer conn.Close()

	log.Printf("Connected to device %v", ip)

	message := fmt.Sprintf("sf: %f", config.Sf, ",tx: %f", config.Tx, ",bw: %f", config.Bandwidth)

	_, err = conn.Write([]byte(message))

	if err != nil {
		log.Fatalln(fmt.Errorf("error sending message: %v", err))
	}
	log.Printf("Sent params: %v", message)
}

func registerClient(deviceID string, conn net.Conn) {
	ServerInst.mutex.Lock()
	defer ServerInst.mutex.Unlock()

	ServerInst.clients[deviceID] = &Client{
		conn:            conn,
		lastInteraction: time.Now().String(),
	}

	log.Printf("Registered client: %s", deviceID)
}

func unregisterClient(deviceID string) {
	ServerInst.mutex.Lock()
	defer ServerInst.mutex.Unlock()

	if client, exists := ServerInst.clients[deviceID]; exists {
		client.conn.Close()
		delete(ServerInst.clients, deviceID)
		log.Printf("Unregistered client: %s", deviceID)
	}
}

func deleteAllClients() {
	ServerInst.mutex.Lock()
	defer ServerInst.mutex.Unlock()
	for _, client := range ServerInst.clients {
		unregisterClient(client.device)
	}
}
