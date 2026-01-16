package service

import (
	"Lora_Esp_Gsm_Gps_project/internal/core"
	"Lora_Esp_Gsm_Gps_project/internal/esp"
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"Lora_Esp_Gsm_Gps_project/internal/postgres"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type PendingMessage struct {
	Destination models.Destination
	Type        handlers.Message
	Message     string
	Timestamp   time.Time
}

type DataService struct {
	packetChan              chan *models.Packet
	Repo                    *postgres.Repo
	interfaceSettingsChange chan *models.Params
	mu                      sync.RWMutex
	isProcessing            bool
	espConnector            *esp.ESPConnector
	clients                 map[net.Conn]models.Destination
	PendingMessages         map[reflect.Type]*PendingMessage
	pendingMessagesLock     sync.RWMutex
}

func NewDataService(connector *esp.ESPConnector) *DataService {
	service := &DataService{
		packetChan:              make(chan *models.Packet, 100),
		Repo:                    nil,
		interfaceSettingsChange: make(chan *models.Params, 10),
		espConnector:            connector,
		clients:                 make(map[net.Conn]models.Destination, 10),
		PendingMessages:         make(map[reflect.Type]*PendingMessage, 5),
	}
	return service
}

func (s *DataService) EspInitializer(connector esp.Connector) error {
	s.espConnector = connector.(*esp.ESPConnector)
	if s.espConnector == nil {
		return errors.New("esp connector is nilptr")
	}
	if !s.espConnector.IsConnected() {
		log.Printf("esp is not connected while initializing service")
		return errors.New("ESP_NOT_CONNECTED_WHILE_INITALIZING_SERVICE")
	}
	s.clients[s.espConnector.Conn] = models.Lora
	go func() {
		err := s.espConnector.ListeningStart(s.handleESPData)
		if err != nil {
			log.Printf("Error starting listening: %v", err)
		}
	}()
	go s.espConnector.MaintainConnection(s.handleESPData)
	return nil
}

func (s *DataService) StartProcessing() {
	go s.processPackets()
	go s.processInterfaceSettingsChange()
	go s.startPendingProcessing()
}

func (s *DataService) GetChannelStatus() (int, int) {
	return len(s.packetChan), cap(s.packetChan)
}

func (s *DataService) GetChannelUsage() float64 {
	current, capacity := s.GetChannelStatus()
	return float64(current) / float64(capacity) * 100
}

func (s *DataService) GetPacketChannel() <-chan *models.Packet {
	return s.packetChan
}

func (s *DataService) ProcessPacketData(buffer string) error {
	parser := handlers.PacketParser{}
	data, err := parser.ParsePacketData(buffer)

	if err != nil {
		log.Printf("Unexpected error while parsing packet: %v", err)
	}

	select {
	case s.packetChan <- &data:
	default:
		fmt.Printf("Channel full, saving data and starting channel drain: %+v\n", data)
		err := s.Repo.Save(&data, s.espConnector.CurrentSession)
		if err != nil {
			log.Printf("Unexpected error while saving data: %v", err)
		}
		go func() {
			err := s.DrainDataChannel()
			if err != nil {
				log.Printf("Error during channel drain: %v", err)
			}
		}()
	}
	log.Printf("Data channel usage :%f", s.GetChannelUsage())

	return nil
}

func (s *DataService) DrainDataChannel() error {
	for i := 0; i < len(s.packetChan); i++ {
		select {
		case packet := <-s.packetChan:
			err := s.Repo.Save(packet, 0)
			if err != nil {
				log.Printf("Error saving packet during drain: %v", err)
			}
		}
	}
	log.Printf("Data channel drain completed")

	return nil
}

func (s *DataService) ProcessInterfaceSettingChange(conn net.Conn, message string) error {
	cleanedMessage := strings.TrimPrefix(message, "SET_SETTINGS: ")
	parts := strings.Split(cleanedMessage, ", ")
	s.clients[conn] = models.Client

	if s.espConnector.Conn == nil || !s.espConnector.IsConnected() {
		return errors.New("ESP_NOT_CONNECTED")
	}

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

	log.Printf("Got params: %+v\n", params)
	s.interfaceSettingsChange <- &params

	return nil
}

func (s *DataService) handleESPData(data string) {
	if s.espConnector.Conn == nil || !s.espConnector.IsConnected() {
		log.Printf("ESP_CONNECTION UNAVAILABLE")
		return
	}
	if data == "" {
		return
	}
	if strings.HasPrefix(data, "ACK") {
		log.Printf("Got ACK: %s", data)
		return
	}
	if strings.HasPrefix(data, "ERROR") {
		log.Printf("ESP Error: %s", data)
		return
	}
	if strings.HasPrefix(data, "IDENTIFY") {
		s.espConnector.SetConnected(true)
		s.onEspConnected()
		return
	}

	err := s.ProcessPacketData(data)
	if err != nil {
		log.Printf("Error processing packet data to chan: %v", err)
	}
}

func (s *DataService) processPackets() {
	s.mu.Lock()
	s.isProcessing = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.isProcessing = false
		s.mu.Unlock()
	}()
	for packet := range s.packetChan {
		err := s.Repo.Save(packet, s.espConnector.CurrentSession)
		if err != nil {
			log.Printf("Error saving packet during processing: %v", err)
		}
		clientConn, _ := s.FindClientConnection()
		s.SendPacketToClient(clientConn, *packet)
	}
}

func (s *DataService) processPendingMessages() {
	s.pendingMessagesLock.Lock()
	defer s.pendingMessagesLock.Unlock()
	for _, pending := range s.PendingMessages {
		switch pending.Type {
		case handlers.SetSettingsMessage{}:
			if s.espConnector.IsConnected() {
				conn, exist := s.FindEspConnection()
				if !exist {
					log.Printf("NO_ESP_CONNECTION FOUND")
				}
				err := s.ProcessInterfaceSettingChange(conn, pending.Message)
				if err != nil {
					log.Printf("Error processing pending settings change: %v", err)
				}
				log.Printf("Pending settings processed successfully")
			} else {
				log.Printf("ESP not connected, skipping pending settings")
				continue
			}
		case handlers.StartMeasurementMessage{}:
			if s.espConnector.IsConnected() {
				conn, exist := s.FindEspConnection()
				if !exist {
					log.Printf("NO_ESP_CONNECTION FOUND")
				}
				msg, err := handlers.ParseClientMessage(s, pending.Message)

				if err != nil {
					log.Printf("Error parsing client message: %v", err)
				}
				switch m := msg.(type) {
				case handlers.StartMeasurementMessage:
					err = s.SendMeasurementCommand(conn, "START_MEASUREMENT", m.SessionId)
					if err != nil {
						log.Printf("Error processing pending settings change: %v", err)
					}
				}
				log.Printf("Pending settings processed successfully")
			} else {
				continue
			}
		default:
			log.Printf("Unexpected pending message type: %s", pending.Type)
		}
	}
}

func (s *DataService) processInterfaceSettingsChange() {
	for params := range s.interfaceSettingsChange {
		if s.espConnector.Conn == nil || !s.espConnector.IsConnected() {
			log.Printf("ESP_CONNECTION UNAVAILABLE")
			return
		}

		err := s.espConnector.SendParamsToDevice(*params)
		if err != nil {
			log.Printf("Error sending params to device: %v", err)
		} else {
			log.Printf("Params sent to device successfully: %+v", params)
		}
	}
}

func (s *DataService) GetMeasurements(requestID int) ([]models.Packet, error) {
	if requestID == 0 {
		var err error
		requestID, err = s.Repo.FindLastRequestId()
		if err != nil {
			log.Printf("Error getting last request id: %v", err)
			return nil, err
		}
	}

	data, err := s.Repo.FindById(requestID)
	if err != nil {
		log.Printf("Error getting measurements for id: %v %d", err, requestID)
		return nil, err
	}

	return data, nil
}

func (s *DataService) SendMeasurementCommand(conn net.Conn, command string, sessionId int) error {
	log.Printf("Connected to device %v", conn.RemoteAddr().String())
	var err error
	if sessionId == 0 {
		_, err = conn.Write([]byte(command))

		if err != nil {
			log.Println(fmt.Errorf("error sending message to esp: %v", err))
			return err
		}
		log.Printf("Sent commands: %v", command)
		return nil
	} else {
		message := fmt.Sprintf("%s SESSION_ID=%d", command, sessionId)
		_, err = conn.Write([]byte(message))

		if err != nil {
			log.Println(fmt.Errorf("error sending message to esp: %v", err))
			return err
		}
		log.Printf("Sent commands: %v", message)
		return nil
	}
}

func (s *DataService) SendMeasurementsToClient(conn net.Conn, data string) error {
	packets, err := s.GetMeasurements(0)
	if err != nil {
		log.Printf("Error getting measurements: %v", err)
		return err
	}
	jsonData, err := json.Marshal(packets)
	if err != nil {
		log.Printf("Error marshalling measurements: %v", err)
		return err
	}
	resp := fmt.Sprintf("MEASUREMENT: %s\n", jsonData)
	_, err = conn.Write([]byte(resp))
	if err != nil {
		log.Println(fmt.Errorf("error sending message to client: %v", err))
		return err
	}
	log.Printf("Sent commands: %v", data)
	return nil
}

func (s *DataService) SendAllSessions(conn net.Conn, data string) error {
	var err error

	resp := fmt.Sprintf("SESSIONS: %s\n", data)
	log.Printf("Sent message: %s", resp)
	_, err = conn.Write([]byte(resp))
	if err != nil {
		log.Println(fmt.Errorf("error sending message to client: %v", err))
		return err
	}
	log.Printf("Sent all sessions data")
	return nil
}

func (s *DataService) GetAllSessions() ([]models.Session, error) {
	sessions, err := s.Repo.GetAllSessions()
	if err != nil {
		log.Printf("Error getting all sessions: %v", err)
		return nil, err
	}
	return sessions, nil
}

func (s *DataService) SaveSession(session models.Session) error {
	err := s.Repo.SaveSession(session)
	if err != nil {
		log.Printf("Error saving session: %v", err)
		return err
	}
	return nil
}

func (s *DataService) RemoveSession(sessionId int) error {
	err := s.Repo.RemoveSession(sessionId)
	if err != nil {
		log.Printf("Error removing session: %v", err)
	}
	return nil
}

func (s *DataService) AddClient(conn net.Conn, destination models.Destination) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clients[conn] = destination
	log.Printf("Client added: %s (type: %s). Total clients: %d",
		conn.RemoteAddr().String(), destination.String(), len(s.clients))
}

func (s *DataService) SendPacketToClient(conn net.Conn, packet models.Packet) {
	_, exists := s.clients[conn]
	if !exists {
		log.Printf("Client not found on connection: %v", conn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	message := fmt.Sprintf("MEASUREMENT: [%s, %d, %f, %f, %f, %f]", packet.Timestamp, packet.RSSI, packet.SNRL, packet.Hdop, packet.Coordinate.Latitude, packet.Coordinate.Longitude)
	_, err := conn.Write([]byte(message + "\n"))
	if err != nil {
		log.Printf("Error sending message to client: %v", err)
		return
	}
	log.Printf("Message %s sent to client %s", message, conn.RemoteAddr().String())
}

func (s *DataService) FindClientConnection() (net.Conn, bool) {
	var clientConn net.Conn
	for conn, dest := range s.clients {
		if dest == models.Client {
			clientConn = conn
		}
	}
	return clientConn, clientConn != nil
}

func (s *DataService) FindEspConnection() (net.Conn, bool) {
	var espConn net.Conn
	for conn, dest := range s.clients {
		if dest == models.Lora {
			espConn = conn
		}
	}
	return espConn, espConn != nil
}

func (s *DataService) AddPendingMessage(message string, destination models.Destination, messageType core.Message) {
	s.pendingMessagesLock.Lock()
	defer s.pendingMessagesLock.Unlock()

	msg := &PendingMessage{
		Destination: destination,
		Type:        messageType,
		Message:     message,
		Timestamp:   time.Now(),
	}

	msgType := reflect.TypeOf(messageType)

	if existing, exists := s.PendingMessages[msgType]; !exists ||
		msg.Timestamp.After(existing.Timestamp) {
		s.PendingMessages[msgType] = msg
		log.Printf("Added new pending message: %v", msg)
	}
	log.Printf("Havent added pending message: %v", msg)
}

func (s *DataService) onEspConnected() {
	log.Printf("ESP connected, processing pending messages")
	s.processPendingMessages()
}

func (s *DataService) startPendingProcessing() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if s.espConnector.IsConnected() {
				s.pendingMessagesLock.Lock()
				s.onEspConnected()
				s.pendingMessagesLock.Unlock()
			}
		}
	}
}
