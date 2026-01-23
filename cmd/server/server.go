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
	"strings"
	"sync"
	"time"
)

type DeviceStatus struct {
	LastUpdated time.Time
	Status      string
}

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
	SlaveState         DeviceStatus
	MasterState        DeviceStatus
}

func NewServer(h core.MeasurementHandler, connector esp.Connector) *Server {
	return &Server{
		clients:            make(map[string]*Client),
		measurementHandler: h,
		connector:          connector,
		SlaveState:         DeviceStatus{},
		MasterState:        DeviceStatus{},
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

	//go s.startConnectionChecker()
	for scanner.Scan() {
		message := scanner.Text()
		log.Printf("Got message: %s", message)
		parsedMsg, err := handlers.ParseClientMessage(s.measurementHandler, message)

		if err != nil {
			log.Printf("Error parsing message: %v", err)
			continue
		}

		log.Printf("Parsed message type: %T", parsedMsg)
		switch msg := parsedMsg.(type) {
		case handlers.SetSettingsMessage:
			id := "settings-change"
			log.Printf("Setting change %s", id)
			if s.connector.IsConnected() {
				s.registerClient(id, conn)
				s.HandleSetSettings(conn, message)
				s.updateClientInteraction(id)
			} else {
				log.Printf("Error branch for SSM")
				s.addWaitingClient(conn)
				log.Printf("Client %s waiting for esp connection, ", id)
				s.measurementHandler.AddPendingMessage(message, models.Lora, handlers.SetSettingsMessage{})
				go func() {
					err := s.waitForEspConnection(conn)
					if err != nil {
						log.Printf("Error waiting for esp connection: %v", err)
					}
				}()
			}

		case handlers.StartMeasurementMessage:
			id := "start-meas"
			log.Printf("Starting handling measurement for %s", id)
			if s.connector.IsConnected() {
				s.handleStartMeasurement(conn, msg.SessionId)
				s.registerClient(id, conn)
				s.updateClientInteraction(id)
			} else {
				log.Printf("Error branch in SMM")
				s.addWaitingClient(conn)
				log.Printf("Client %s waiting for esp connection, ", id)
				s.measurementHandler.AddPendingMessage(message, models.Lora, handlers.StartMeasurementMessage{})
				go func() {
					err := s.waitForEspConnection(conn)
					if err != nil {
						log.Printf("Error waiting for esp connection: %v", err)
					}
				}()
			}

		case handlers.StopMeasurementMessage: // command unused
			return nil
		case handlers.GetMeasurementSessionsMessage: //esp not used here
			s.HandleGetMeasurementSessions(conn)
			id := "get-sessions"
			s.registerClient(id, conn)
			s.updateClientInteraction(id)

		case handlers.AddSessionMessage: //esp not used here
			s.HandleAddSession(conn, msg.Session)
			id := "add-session"
			s.registerClient(id, conn)
			s.updateClientInteraction(id)

		case handlers.RemoveSessionMessage: // esp not used here
			s.HandleRemoveSession(conn, msg.SessionId)
			id := "remove-session"
			s.registerClient(id, conn)
			s.updateClientInteraction(id)

		case handlers.EspMessage:
			once := sync.Once{}
			once.Do(func() { go s.StartStatusMonitor() })
			log.Printf("Esp message received: %v", msg)
			s.HandleEspConnection(conn)
			log.Printf("New status for master: %v", "CONNECTED")
			err = s.SendMasterStatus("CONNECTED")
			id := "identify_esp"
			s.registerClient(id, conn)
			s.updateClientInteraction(id)
		case handlers.IncomingMeasurementMessage:
			log.Printf("Received incoming measurement: %v", msg)
			err = s.measurementHandler.ProcessPacketData(msg.Data)
			id := "meas_esp"
			s.registerClient(id, conn)
			s.updateClientInteraction(id)
		case handlers.AckMessage:
			log.Printf("Received ack message: %v", msg)
		case handlers.ErrMessage:
			log.Printf("Received err message from master: %v", msg)
		case handlers.UnknownMessage:
			log.Printf("Unknown message type: %v", message)
		case handlers.MasterStatusMessage:
			log.Printf("New status for master: %v", msg.Status)
			err = s.SendMasterStatus(msg.Status)
			if err != nil {
				log.Printf("Error sending master status: %v", err)
			}
		case handlers.SlaveStatusMessage:
			log.Printf("New status for slave: %v", msg.Status)
			err = s.SendSlaveStatus(msg.Status)
			if err != nil {
				log.Printf("Error sending master status: %v", err)
			}
		default:
			log.Printf("Unhandled message type: %T", msg)
		}
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
	timeout := time.After(30 * time.Second)
	log.Printf("Waiting for esp connection to establish")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			s.removeWaitingClient(conn)
			log.Printf("Client %s not waiting for esp connection to establish, timeout", conn.RemoteAddr())
			return errors.New("ESP connection timeout")
		case <-ticker.C:
			if s.connector.IsConnected() {
				s.removeWaitingClient(conn)
				s.notifyWaitingClients()
				return nil
			}
		}
	}
}

func (s *Server) addWaitingClient(conn net.Conn) {
	s.waitListMutex.Lock()
	s.waitList = append(s.waitList, conn)
	log.Printf("Adding waiting client: %s", conn.RemoteAddr())
	s.waitListMutex.Unlock()
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

func (s *Server) removeAllWaitingClients() {
	s.waitListMutex.Lock()
	defer s.waitListMutex.Unlock()
	for _, connection := range s.waitList {
		s.removeWaitingClient(connection)
	}
}

func (s *Server) notifyWaitingClients() {
	s.waitListMutex.Lock()
	defer s.waitListMutex.Unlock()
	//
	//for _, conn := range s.waitList {
	//	conn.Write([]byte("ESP_CONNECTED_PROCEEDING\n"))
	//}
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
			//conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
		}
	}()
}

func (s *Server) handleStartMeasurement(conn net.Conn, sessionId int) {
	go func() {
		err := s.connector.SendCommand("START_MEASUREMENT", sessionId)
		if err != nil {
			log.Printf("Error processing interface request: %v", err)
			//conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
		}
	}()
	go func() {
		packets, err := s.measurementHandler.GetAllSessionMeasurements(sessionId)
		if err != nil {
			log.Printf("Error getting session measurements: %v", err)
		}
		if packets != nil {
			err = s.measurementHandler.SendSessionPackets(sessionId, packets)
			if err != nil {
				log.Printf("Error sending session packets: %v", err)
			}
		}
	}()
}

func (s *Server) HandleGetMeasurementSessions(conn net.Conn) {
	go func() {
		sessions, err := s.measurementHandler.GetAllSessions()
		if err != nil {
			log.Printf("Error getting sessions: %v", err)
			//conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
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
			//conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
			return
		}
		err = s.measurementHandler.SendAllSessions(conn, sessionsStr)
		if err != nil {
			log.Printf("Error sending sessions to client: %v", err)
			//conn.Write([]byte("ERROR OCCURED ON SERVER: " + err.Error() + "\n"))
			return
		}
	}()
}

func (s *Server) HandleAddSession(conn net.Conn, session models.Session) {
	err := s.measurementHandler.SaveSession(session)
	if err != nil {
		log.Printf("Error saving session: %v", err)
		return
	}
	log.Printf("Session saved: %v", session)
}

func (s *Server) HandleRemoveSession(conn net.Conn, sessionId int) {
	err := s.measurementHandler.RemoveSession(sessionId)
	if err != nil {
		log.Printf("Error removing session: %v", err)
		return
	}
	log.Printf("Session removed: %v", sessionId)
}

func (s *Server) HandleEspConnection(conn net.Conn) {
	s.connector.SetConn(nil)

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
}

func (s *Server) SendMasterStatus(status string) error {
	if s.clients == nil {
		log.Printf("No clients connected, skip sending master status")
		return nil
	}

	s.MasterState.Status = status
	s.MasterState.LastUpdated = time.Now()

	return s.SendStatus("MASTER_STATUS " + status)
}

func (s *Server) SendSlaveStatus(status string) error {
	if s.clients == nil {
		log.Printf("No clients connected, skip sending slave status")
		return nil
	}

	s.SlaveState.Status = status
	s.SlaveState.LastUpdated = time.Now()

	return s.SendStatus("SLAVE_STATUS " + status)
}

func (s *Server) SendStatus(status string) error {

	if client, exists := s.clients["settings-change"]; exists {
		client.mutex.RLock()
		defer client.mutex.RUnlock()
		if client.isActive {
			conn := client.conn
			_, err := conn.Write([]byte(status))
			log.Printf("Sending status to client: %v, %s", client, status)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return errors.New("no interface connected")
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

func (s *Server) StartStatusMonitor() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			s.checkAndUpdateStatus()
		}
	}()
}

func (s *Server) checkAndUpdateStatus() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	now := time.Now()

	if !s.MasterState.LastUpdated.IsZero() && now.Sub(s.MasterState.LastUpdated) > 120*time.Second && s.MasterState.Status != "DISCONNECTED" {
		log.Printf("Master статус не обновлялся более 2 минут, меняем на DISCONNECTED")
		log.Printf("Master state before: %s, after: %s", s.MasterState, "DISCONNECTED")

		err := s.SendMasterStatus("DISCONNECTED")
		if err != nil {
			log.Printf("Error sending master status: %v", err)
		}
	} else {
		log.Printf("Timeout expired, skip changing master status, stay DISCONNECTED")
	}

	if !s.SlaveState.LastUpdated.IsZero() && now.Sub(s.SlaveState.LastUpdated) > 120*time.Second && s.SlaveState.Status != "DISCONNECTED" {
		log.Printf("Slave статус не обновлялся более 2 минут, меняем на DISCONNECTED")
		log.Printf("Slave state before: %s, after: %s", s.SlaveState.Status, "DISCONNECTED")
		err := s.SendSlaveStatus("DISCONNECTED")
		if err != nil {
			log.Printf("Error sending master status: %v", err)
		}
	} else {
		log.Printf("Timeout expired, skip changing slave status, stay DISCONNECTED")
	}
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
