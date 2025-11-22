package server

import (
	"Lora_Esp_Gsm_Gps_project/internal/core"
	"Lora_Esp_Gsm_Gps_project/internal/esp"
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"bufio"
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
	isActive        bool
	mutex           sync.RWMutex
}

type Server struct {
	clients            map[string]*Client
	mutex              sync.RWMutex
	measurementHandler core.MeasurementHandler
	connector          esp.Connector
	waitList           []net.Conn
	waitListMutex      sync.RWMutex
}

func NewServer(h core.MeasurementHandler, connector esp.Connector) *Server {
	return &Server{
		clients:            make(map[string]*Client),
		measurementHandler: h,
		connector:          connector,
	}
}

func (s *Server) StartServer(port string) error {

	log.Printf("Starting server on port %v", port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))

	if err != nil {
		log.Println(fmt.Errorf("error starting server: %v", err))
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
			log.Println(fmt.Errorf("error accepting connection: %v", err.Error()))
		}
		log.Printf("Accepted connection from %v", conn.RemoteAddr())
		go func() {
			err := s.HandleConnection(conn)
			if err != nil {
				log.Printf("Error handling connection: %v", err)
			}
		}()
	}
}

func (s *Server) HandleConnection(conn net.Conn) error {
	log.Printf("Client connected from %s", conn.RemoteAddr())
	scanner := bufio.NewScanner(conn)
	count := 0

	//go s.startConnectionChecker()
	for scanner.Scan() {
		message := scanner.Text()

		parsedMsg, err := handlers.ParseClientMessage(s.measurementHandler, message)

		if err != nil {
			log.Printf("Error parsing message: %v", err)
			conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
			continue
		}
		switch msg := parsedMsg.(type) {
		case handlers.SetSettingsMessage:
			id := "settings-change" + strconv.Itoa(count)
			if s.connector.IsConnected() {
				s.registerClient(id, conn)
				s.HandleSetSettings(conn, message)
				s.updateClientInteraction(id)
			} else {
				s.waitListMutex.Lock()
				defer s.waitListMutex.Unlock()
				s.waitList = append(s.waitList, conn)
				log.Printf("Client %s waiting for esp connection, ", id)
				s.measurementHandler.AddPendingMessage(message, models.Lora)
				return s.waitForEspConnection(conn)
			}

		case handlers.StartMeasurementMessage:
			id := "start-meas" + strconv.Itoa(count)
			if s.connector.IsConnected() {
				s.handleStartMeasurement(conn, msg.SessionId)
				s.registerClient(id, conn)
				s.updateClientInteraction(id)
			} else {
				s.waitListMutex.Lock()
				defer s.waitListMutex.Unlock()
				s.waitList = append(s.waitList, conn)
				log.Printf("Client %s waiting for esp connection, ", id)
				s.measurementHandler.AddPendingMessage(message, models.Lora)
				return s.waitForEspConnection(conn)
			}

		case handlers.StopMeasurementMessage: // command unused
			return nil
		case handlers.GetMeasurementSessionsMessage: //esp not used here
			s.HandleGetMeasurementSessions(conn)
			id := "get-sessions" + strconv.Itoa(count)
			s.registerClient(id, conn)
			s.updateClientInteraction(id)

		case handlers.AddSessionMessage: //esp not used here
			s.HandleAddSession(conn, msg.Session)
			id := "add-session" + strconv.Itoa(count)
			s.registerClient(id, conn)
			s.updateClientInteraction(id)

		case handlers.RemoveSessionMessage: // esp not used here
			s.HandleRemoveSession(conn, msg.SessionId)
			id := "remove-session" + strconv.Itoa(count)
			s.registerClient(id, conn)
			s.updateClientInteraction(id)

		case handlers.UnknownMessageMessage:
			log.Printf("Unknown message type: %v", message)

		case handlers.EspMessage:
			log.Printf("Esp message received: %v", msg)
			s.HandleEspConnection(conn)
			id := "identify_esp" + strconv.Itoa(count)
			s.registerClient(id, conn)
			s.updateClientInteraction(id)

		default:
			log.Printf("Unhandled message type: %T", msg)
		}
		count++
		log.Printf("Received message: %v", message)
	}
	//s.checkIsConnectionsAlive()
	if err := scanner.Err(); err != nil {
		log.Println("Error reading:", err.Error())
		return err
	}
	return nil
}

func (s *Server) registerClient(clientId string, conn net.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.clients[clientId] = &Client{
		conn:            conn,
		lastInteraction: time.Now().String(),
		isActive:        true,
	}

	log.Printf("Registered client: %s", clientId)
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
		log.Printf("Deregistered client: %s", deviceID)
	}
}

func (s *Server) waitForEspConnection(conn net.Conn) error {
	for {
		select {
		case <-time.After(30 * time.Second):
			conn.Write([]byte("ERROR: ESP connection timeout\n"))
			s.removeWaitingClient(conn)
			return errors.New("ESP connection timeout")
		default:
			if s.connector.IsConnected() {
				conn.Write([]byte("ESP_CONNECTED\n"))
				s.removeWaitingClient(conn)
				return nil
			} else {
				conn.Write([]byte("ERROR: ESP connection timeout\n"))
				s.removeWaitingClient(conn)
				return errors.New("ESP connection timeout")
			}
		}
	}
}

func (s *Server) addWaitingClient(conn net.Conn) {
	s.waitListMutex.Lock()
	defer s.waitListMutex.Unlock()
	s.waitList = append(s.waitList, conn)
}

func (s *Server) removeWaitingClient(conn net.Conn) {
	s.waitListMutex.Lock()
	defer s.waitListMutex.Unlock()
	for i, connection := range s.waitList {
		if connection == conn {
			s.waitList = append(s.waitList[:i], s.waitList[i+1:]...)
			break
		}
	}
}

func (s *Server) notifyWaitingClients() {
	s.waitListMutex.Lock()
	defer s.waitListMutex.Unlock()

	for _, conn := range s.waitList {
		conn.Write([]byte("ESP_CONNECTED_PROCEEDING\n"))
	}
	s.waitList = nil
}

//func (s *Server) checkIsConnectionsAlive() {
//	s.mutex.Lock()
//	defer s.mutex.Unlock()
//	for deviceID, client := range s.clients {
//		ok := client.isConnectionAlive()
//		if !ok {
//			log.Printf("Client %s inactive for over 5 minutes, disconnecting", deviceID)
//			err := client.conn.Close()
//			if err != nil {
//				log.Printf("Error closing connection for client %s: %v", deviceID, err)
//				continue
//			}
//			delete(s.clients, deviceID)
//			log.Printf("Unregistered inactive client: %s", deviceID)
//		}
//	}
//}

func (s *Server) updateClientInteraction(clientID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if client, exists := s.clients[clientID]; exists {
		client.lastInteraction = time.Now().String()
		client.isActive = true
	}
}

func (s *Server) deleteAllClients() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, client := range s.clients {
		s.unregisterClient(client.device)
	}
}

func (s *Server) HandleSetSettings(conn net.Conn, message string) {
	go func() {
		err := s.measurementHandler.ProcessInterfaceSettingChange(conn, message)
		if err != nil {
			log.Printf("Error processing interface request: %v", err)
			conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
		}
	}()
}

func (s *Server) handleStartMeasurement(conn net.Conn, sessionId int) {
	go func() {
		err := s.connector.SendCommand("START_MEASUREMENT", sessionId)
		if err != nil {
			log.Printf("Error processing interface request: %v", err)
			conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
		}
	}()
}

func (s *Server) HandleGetMeasurementSessions(conn net.Conn) {
	go func() {
		sessions, err := s.measurementHandler.GetAllSessions()
		if err != nil {
			log.Printf("Error getting sessions: %v", err)
			conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
			return
		}
		var parts []string
		for _, session := range sessions {
			str := fmt.Sprintf("[%d, %s, %s, %s, %d]", session.Id, session.Name, session.StartTime, session.EndTime, session.Count)
			parts = append(parts, str)
		}
		sessionsStr := strings.Join(parts, ", ")
		if err != nil {
			log.Printf("Error joining sessions: %v", err)
			conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
			return
		}
		err = s.measurementHandler.SendAllSessions(conn, sessionsStr)
		if err != nil {
			log.Printf("Error sending sessions to client: %v", err)
			conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
			return
		}
	}()
}

func (s *Server) HandleAddSession(conn net.Conn, session models.Session) {
	err := s.measurementHandler.SaveSession(session)
	if err != nil {
		log.Printf("Error saving session: %v", err)
		conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
		return
	}
	log.Printf("Session saved: %v", session)
}

func (s *Server) HandleRemoveSession(conn net.Conn, sessionId int) {
	err := s.measurementHandler.RemoveSession(sessionId)
	if err != nil {
		log.Printf("Error removing session: %v", err)
		conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
		return
	}
	log.Printf("Session removed: %v", sessionId)
}

func (s *Server) HandleEspConnection(conn net.Conn) {
	s.connector.SetConnected(true)
	s.connector.SetIP(conn.RemoteAddr().String())
	s.connector.SetConn(conn)
	err := s.measurementHandler.EspInitializer(s.connector)
	if err != nil {
		log.Printf("Error initializing Esp: %v", err)
		return
	}
	log.Printf("Connection from esp: %v", conn.RemoteAddr().String())
	log.Printf("ESP_OK")
	_, err = conn.Write([]byte("OK\n"))
	if err != nil {
		return
	}
}

func (c *Client) setInactive() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.isActive = false
}

func (c *Client) isConnectionAlive() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	inter, err := handlers.ParseTime(c.lastInteraction)
	if err != nil {
		log.Printf("Error parsing last interaction time: %v", err)
		return false
	}
	return c.isActive && time.Since(inter) < 5*time.Minute
}

//func (s *Server) startConnectionChecker() {
//	ticker := time.NewTicker(1 * time.Minute)
//	defer ticker.Stop()
//
//	for {
//		select {
//		case <-ticker.C:
//			s.checkIsConnectionsAlive()
//		}
//	}
//}

// trash
//if strings.HasPrefix(body, "SET_SETTINGS") {
//go func() {
//err := s.measurementHandler.ProcessInterfaceSettingChange(message)
//if err != nil {
//log.Printf("Error processing interface request: %v", err)
//}
//}()
//} else if strings.HasPrefix(body, "START_MEASUREMENT") {
//s.registerClient("start_meas"+strconv.Itoa(count), conn)
//cleaned := strings.TrimPrefix(message, "START_MEASUREMENT: ")
//
//sessionID, err := strconv.Atoi(strings.TrimSpace(cleaned))
//if err != nil {
//return fmt.Errorf("invalid request ID: %w", err)
//}
//go func() {
//espCfg := configs.LoadEspConfig()
//err := s.measurementHandler.SendMeasurementCommand(espCfg.EspIP, espCfg.EspPort, "START_MEASUREMENT", sessionID)
//if err != nil {
//log.Printf("Error processing interface request: %v", err)
//}
//}()
//} else if strings.HasPrefix(body, "STOP_MEASUREMENT") {
//s.registerClient("stop_meas"+strconv.Itoa(count), conn)
//go func() {
//espCfg := configs.LoadEspConfig()
//err := s.measurementHandler.SendMeasurementCommand(espCfg.EspIP, espCfg.EspPort, "STOP_MEASUREMENT", 0)
//if err != nil {
//log.Printf("Error processing interface request: %v", err)
//}
//}()
//} else if strings.HasPrefix(message, "GET_MEASUREMENT_SESSIONS") {
//s.registerClient("get_sessions"+strconv.Itoa(count), conn)
//go func() {
//sessions, err := s.measurementHandler.GetAllSessions()
//if err != nil {
//log.Printf("Error getting sessions: %v", err)
//return
//}
//var parts []string
//for _, session := range sessions {
//str := fmt.Sprintf("[%d, %s, %s, %s, %d]", session.Id, session.Name, session.StartTime, session.EndTime, session.Count)
//parts = append(parts, str)
//}
//sessionsStr := strings.Join(parts, ", ")
//if err != nil {
//log.Printf("Error joining sessions: %v", err)
//return
//}
//var clientPort string
//var clientIp string
//ip := conn.RemoteAddr().(*net.TCPAddr)
//{
//clientPort = strconv.Itoa(ip.Port)
//clientIp = ip.IP.String()
//log.Printf("Client IP: %s", clientIp)
//log.Printf("Client Port: %s", clientPort)
//}
//err = s.measurementHandler.SendAllSessions(conn, sessionsStr)
//if err != nil {
//log.Printf("Error sending sessions to client: %v", err)
//return
//}
//}()
//} else if strings.HasPrefix(message, "ADD_SESSION") {
//msg := strings.TrimPrefix(message, "ADD_SESSION: ")
//session, err := handlers.ParseSession(msg)
//if err != nil {
//log.Printf("Error parsing session: %v", err)
//return err
//}
//err = s.measurementHandler.SaveSession(session)
//if err != nil {
//log.Printf("Error saving session: %v", err)
//return err
//}
//log.Printf("Session saved: %v", session)
//
//} else if strings.HasPrefix(message, "REMOVE_SESSION") {
//sessionId, err := handlers.ParseRemoveSessionMessage(message)
//if err != nil {
//log.Printf("Error parsing remove session message: %v", err)
//return err
//}
//err = s.measurementHandler.RemoveSession(int32(sessionId))
//if err != nil {
//log.Printf("Error removing session: %v", err)
//return err
//}
//log.Printf("Session removed: %v", sessionId)
//} else {
//log.Printf("Error processing message, no such command: %v", message)
//}
