package server

import (
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/core"
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"bufio"
	"encoding/json"
	"errors"
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
		parsedMsg, err := handlers.ParseClientMessage(s.measurementHandler, message)
		if err != nil {
			log.Printf("Error parsing message: %v", err)
		}
		body, err := s.handleParsedMessage(parsedMsg)
		if err != nil {
			log.Printf("Error getting message type message: %v", err)
		}
		if strings.HasPrefix(body, "SET_SETTINGS") {
			s.registerClient("settings-change"+strconv.Itoa(count), conn)
			go func() {
				err := s.measurementHandler.ProcessInterfaceSettingChange(message)
				if err != nil {
					log.Printf("Error processing interface request: %v", err)
				}
			}()
		} else if strings.HasPrefix(body, "START_MEASUREMENT") {
			s.registerClient("start_meas"+strconv.Itoa(count), conn)
			cleaned := strings.TrimPrefix(message, "START_MEASUREMENT: ")

			sessionID, err := strconv.Atoi(strings.TrimSpace(cleaned))
			if err != nil {
				return fmt.Errorf("invalid request ID: %w", err)
			}
			go func() {
				espCfg := configs.LoadEspConfig()
				err := s.measurementHandler.SendMeasurementCommand(espCfg.EspIP, espCfg.EspPort, "START_MEASUREMENT", sessionID)
				if err != nil {
					log.Printf("Error processing interface request: %v", err)
				}
			}()
		} else if strings.HasPrefix(body, "STOP_MEASUREMENT") {
			s.registerClient("stop_meas"+strconv.Itoa(count), conn)
			go func() {
				espCfg := configs.LoadEspConfig()
				err := s.measurementHandler.SendMeasurementCommand(espCfg.EspIP, espCfg.EspPort, "STOP_MEASUREMENT", 0)
				if err != nil {
					log.Printf("Error processing interface request: %v", err)
				}
			}()
			//} else if strings.HasPrefix(body, "GET_MEASUREMENT:") {
			//	s.registerClient("get_meas"+strconv.Itoa(count), conn)
			//	go func() {
			//		clientCfg := configs.LoadClientConfig()
			//		data := strings.TrimPrefix(body, "GET_MEASUREMENT:")
			//		err := s.measurementHandler.SendMeasurementsToClient(clientCfg.ClientIp, clientCfg.ClientPort, data)
			//		if err != nil {
			//			log.Printf("Error processing interface request: %v", err)
			//		}
			//	}()
		} else if strings.HasPrefix(message, "GET_MEASUREMENTS_SESSIONS") {
			s.registerClient("get_sessions"+strconv.Itoa(count), conn)
			go func() {
				sessions, err := s.measurementHandler.GetAllSessions()
				if err != nil {
					log.Printf("Error getting sessions: %v", err)
					return
				}
				clientCfg := configs.LoadClientConfig()
				jsonData, err := json.Marshal(sessions)
				if err != nil {
					log.Printf("Error marshalling sessions: %v", err)
					return
				}
				err = s.measurementHandler.SendAllSessions(clientCfg.ClientIp, clientCfg.ClientPort, string(jsonData))
				if err != nil {
					log.Printf("Error sending sessions to client: %v", err)
					return
				}
			}()
		} else if strings.HasPrefix(message, "ADD_SESSION") {
			msg := strings.TrimPrefix(message, "ADD_SESSION: ")
			session, err := handlers.ParseSession(msg)
			if err != nil {
				log.Printf("Error parsing session: %v", err)
				return err
			}
			err = s.measurementHandler.SaveSession(session)
			if err != nil {
				log.Printf("Error saving session: %v", err)
				return err
			}
			log.Printf("Session saved: %v", session)

		} else if strings.HasPrefix(message, "REMOVE_SESSION") {
			sessionId, err := handlers.ParseRemoveSessionMessage(message)
			if err != nil {
				log.Printf("Error parsing remove session message: %v", err)
				return err
			}
			err = s.measurementHandler.RemoveSession(int32(sessionId))
			if err != nil {
				log.Printf("Error removing session: %v", err)
				return err
			}
			log.Printf("Session removed: %v", sessionId)
		} else {
			log.Printf("Error processing message, no such command: %v", message)
		}
		count++
		log.Printf("Received message: %v", message)
	}
	if err := scanner.Err(); err != nil {
		log.Println("Error reading:", err.Error())
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

func (s *Server) handleParsedMessage(msg handlers.Message) (string, error) {
	switch m := msg.(type) {
	case handlers.SetSettingsMessage:
		return fmt.Sprintf("SET_SETTINGS sf: %f, tx: %f, bw: %f", m.Params.Sf, m.Params.Tx, m.Params.Bandwidth), nil

	case handlers.GetDataMessage:
		if len(m.Data) == 0 {
			return `{"status": "empty"}`, errors.New("no data received")
		}
		response := struct {
			Status    string          `json:"status"`
			Count     int             `json:"count"`
			RequestID int             `json:"request_id,omitempty"`
			Data      []models.Packet `json:"data"`
			Timestamp string          `json:"timestamp"`
		}{
			Status:    "success",
			Count:     len(m.Data),
			Data:      m.Data,
			Timestamp: time.Now().Format(time.RFC3339),
		}
		if len(m.Data) > 0 {
			response.RequestID = m.Data[0].RequestId
		}

		jsonData, err := json.Marshal(response)
		if err != nil {
			return "", fmt.Errorf("error marshaling data: %v", err)
		}
		toSend := "GET_MEASUREMENT: " + string(jsonData)
		return toSend, nil

	case handlers.StartMeasurementMessage:
		log.Printf("MEASUREMENT_START for request %d", m.RequestId)
		return fmt.Sprintf("START_MEASUREMENT: %d", m.RequestId), nil

	case handlers.StopMeasurementMessage:
		log.Printf("MEASUREMENT_STOP%d", m.RequestId)
		return fmt.Sprintf("MEASUREMENT_STOP%d", m.RequestId), nil

	case handlers.UnknownMessageMessage:
		return "UNKNOWN_COMMAND", nil

	default:
		return "", fmt.Errorf("unhandled message type: %T", msg)
	}
}
