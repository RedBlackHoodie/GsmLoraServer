package esp

import (
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ESPConnector struct {
	IP          string
	Port        string
	conn        net.Conn
	isConnected bool
	mutex       sync.RWMutex
}

type Message interface {
	Type() string
}

type MeasurementMessage struct {
	Data      models.Packet
	sessionId int
}

type NoSignalErrorMessage struct {
	Error string
}

type UnknownMessage struct{}

func (m MeasurementMessage) Type() string   { return "MEASUREMENT" }
func (m NoSignalErrorMessage) Type() string { return "NO_SIGNAL" }
func (m UnknownMessage) Type() string       { return "UNKNOWN" }

type GetMessage struct {
	What string
}

func NewESPConnector() *ESPConnector {
	return &ESPConnector{
		isConnected: false,
	}
}

func (e *ESPConnector) Connect(ip, port string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	address := fmt.Sprintf("%s:%s", ip, port)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		e.isConnected = false
		return fmt.Errorf("failed to connect to ESP32 at %s: %v", address, err)
	}

	e.IP = ip
	e.Port = port
	e.conn = conn
	e.isConnected = true

	log.Printf("Successfully connected to ESP32 at %s", address)
	return nil
}

func (e *ESPConnector) SendCommand(command string, sessionId int) error {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if !e.isConnected || e.conn == nil {
		return fmt.Errorf("not connected to ESP32")
	}

	var message string
	if sessionId == 0 {
		message = command
	} else {
		message = fmt.Sprintf("%s SESSION_ID=%d", command, sessionId)
	}

	_, err := e.conn.Write([]byte(message + "\n"))
	if err != nil {
		e.isConnected = false
		return fmt.Errorf("failed to send command to ESP32: %v", err)
	}

	log.Printf("Command sent to ESP32: %s", message)
	return nil
}

func (e *ESPConnector) SendParamsToDevice(params models.Params) error {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if !e.isConnected || e.conn == nil {
		return fmt.Errorf("not connected to ESP32")
	}

	message := fmt.Sprintf("SET_SETTINGS: SF=%.1f, TX=%.1f, BW=%.1f", params.Sf, params.Tx, params.Bandwidth)

	_, err := e.conn.Write([]byte(message + "\n"))
	if err != nil {
		e.isConnected = false
		return fmt.Errorf("failed to send params to ESP32: %v", err)
	}

	log.Printf("Params sent to ESP32: %s", message)
	return nil
}

func (e *ESPConnector) ProcessInterfaceSettingChange(message string) error {
	cleanedMessage := strings.TrimPrefix(message, "SET_SETTINGS: ")
	parts := strings.Split(cleanedMessage, ", ")

	params := models.Params{}

	for _, part := range parts {
		keyVal := strings.Split(part, "=")
		if len(keyVal) != 2 {
			continue
		}

		key := strings.TrimSpace(keyVal[0])
		value := strings.TrimSpace(keyVal[1])

		val, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return fmt.Errorf("invalid value for %s: %w", key, err)
		}

		switch key {
		case "SF":
			params.Sf = float32(val)
		case "TX":
			params.Tx = float32(val)
		case "BW":
			params.Bandwidth = float32(val)
		}
	}

	log.Printf("Parsed params: %+v", params)
	return e.SendParamsToDevice(params)
}

func (e *ESPConnector) IsConnected() bool {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.isConnected
}

func (e *ESPConnector) Close() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.conn != nil {
		err := e.conn.Close()
		e.conn = nil
		e.isConnected = false
		return err
	}
	return nil
}

func (e *ESPConnector) MaintainConnection(ip, port string, dataHandler func(string)) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !e.IsConnected() {
			log.Printf("Attempting to reconnect to ESP32...")
			err := e.Connect(ip, port)
			if err != nil {
				log.Printf("Failed to reconnect to ESP32: %v", err)
			} else {
				err = e.ListeningStart(dataHandler)
				if err != nil {
					return
				}
			}
		}
	}
}

func (e *ESPConnector) ListeningStart(dataHandler func(string)) error {
	reader := bufio.NewReader(e.conn)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			e.mutex.Lock()
			e.isConnected = false
			e.conn = nil
			e.mutex.Unlock()
			log.Printf("error reading from ESP32: %v", err)
		}
		if message == "" {
			continue
		}
		log.Printf("Received message from ESP32: %s", message)
		dataHandler(message)
	}
}
