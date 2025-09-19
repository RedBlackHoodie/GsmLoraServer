package server

import (
	"Lora_Esp_Gsm_Gps_project/configs"
	"bufio"
	"fmt"
	"log"
	"net"
)

const cfg = configs.LoadConfig()

func StartServer() error {
	port := cfg.Port

	log.Printf("Starting server on port %v", port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Fatalln(fmt.Errorf("error starting server: %v", err))
	}
	defer listener.Close()
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
	for scanner.Scan() {
		message := scanner.Text()
		buffer = append(buffer, message...)
		log.Printf("Received message: %v", message)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading:", err.Error())
		return err
	}
	return nil
}
