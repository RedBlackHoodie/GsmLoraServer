package service

import (
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"Lora_Esp_Gsm_Gps_project/internal/postgres"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DataService struct {
	packetChan              chan *models.Packet
	Repo                    *postgres.Repo
	interfaceSettingsChange chan *models.Params
	mu                      sync.RWMutex
	isProcessing            bool
}

func NewDataService() *DataService {
	service := &DataService{
		packetChan:              make(chan *models.Packet, 100),
		Repo:                    nil,
		interfaceSettingsChange: make(chan *models.Params, 10),
	}

	return service
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
		err := s.Repo.Save(&data)
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
			err := s.Repo.Save(packet)
			if err != nil {
				log.Printf("Error saving packet during drain: %v", err)
			}
		}
	}
	log.Printf("Data channel drain completed")

	return nil
}

func (s *DataService) ProcessInterfaceSettingChange(message string) error {
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

	log.Printf("Got params: %+v\n", params)
	s.interfaceSettingsChange <- &params

	return nil
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
		err := s.Repo.Save(packet)
		if err != nil {
			log.Printf("Error saving packet during processing: %v", err)
		}
	}
}

func (s *DataService) processInterfaceSettingsChange() {
	config := configs.LoadEspConfig()
	for params := range s.interfaceSettingsChange {
		err := handlers.SendParamsToDevice(config.EspIP, config.EspPort, *params)
		if err != nil {
			log.Printf("Error sending params to device: %v", err)
		} else {
			log.Printf("Params sent to device successfully: %+v", params)
		}
	}
}

func (s *DataService) GetMeasurements(requestID int32) ([]models.Packet, error) {
	if requestID == 0 {
		var err error
		requestID, err = s.Repo.FindLastRequestId()
		if err != nil {
			log.Printf("Error getting last request id: %v", err)
			return nil, err
		}
	}

	data, err := s.Repo.FindById(int32(requestID))
	if err != nil {
		log.Printf("Error getting measurements for id: %v %d", err, requestID)
		return nil, err
	}

	return data, nil
}

func (s *DataService) SendMeasurementCommand(ip, port, command string, sessionId int) error {
	target := ip + ":" + port
	timeout := 10 * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		log.Println(fmt.Errorf("error connecting to device: %v", err))
		return err
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	log.Printf("Connected to device %v", ip)
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

func (s *DataService) SendMeasurementsToClient(ip, port, data string) error {
	target := ip + ":" + port
	timeout := 10 * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		log.Println(fmt.Errorf("error connecting to device: %v", err))
		return err
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)
	log.Printf("Connected to device %v", ip)
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

func (s *DataService) SendAllSessions(ip, port, data string) error {
	target := ip + ":" + port
	var conn net.Conn
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		timeout := time.Duration(attempt) * 5 * time.Second
		conn, err = net.DialTimeout("tcp", target, timeout)
		if err == nil {
			log.Printf("got error while retrying to connect: %v", err)
		}
		log.Printf("Attempt %d failed: %v", attempt, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)
	log.Printf("Connected to device %v", ip)
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

func (s *DataService) RemoveSession(sessionId int32) error {
	err := s.Repo.RemoveSession(sessionId)
	if err != nil {
		log.Printf("Error removing session: %v", err)
	}
	return nil
}
