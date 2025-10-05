package service

import (
	"Lora_Esp_Gsm_Gps_project/internal/esp"
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"Lora_Esp_Gsm_Gps_project/internal/postgres"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

type DataService struct {
	packetChan              chan *models.Packet
	Repo                    *postgres.Repo
	interfaceSettingsChange chan *models.Params
	mu                      sync.RWMutex
	isProcessing            bool
	espConnector            *esp.ESPConnector
	clients                 map[net.Conn]models.Destination
}

func NewDataService(connector *esp.ESPConnector) *DataService {
	service := &DataService{
		packetChan:              make(chan *models.Packet, 100),
		Repo:                    nil,
		interfaceSettingsChange: make(chan *models.Params, 10),
		espConnector:            connector,
		clients:                 make(map[net.Conn]models.Destination, 10),
	}
	return service
}

func (s *DataService) EspInitializer() {
	err := s.espConnector.Connect(s.espConnector.IP, s.espConnector.Port)
	if err != nil {
		log.Printf("Error connecting to ESP: %v", err)
		return
	}
	s.clients[s.espConnector.Conn] = models.Lora
	go func() {
		err = s.espConnector.ListeningStart(s.handleESPData)
		if err != nil {
			log.Printf("Error starting listening: %v", err)
		}
	}()
	go s.espConnector.MaintainConnection(s.espConnector.IP, s.espConnector.Port, s.handleESPData)
}

func (s *DataService) StartProcessing() {
	go s.processPackets()
	go s.processInterfaceSettingsChange()
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
		conn.Write([]byte("ESP_NOT_CONNECTED"))
		return errors.New("ESP_NOT_CONNECTED")
	} else {
		conn.Write([]byte("ESP_CONNECTED"))
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

func (s *DataService) SendToClient(conn net.Conn, message string) {
	_, exists := s.clients[conn]
	if !exists {
		log.Printf("Client not found on connection: %v", conn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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
