package service

import (
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"Lora_Esp_Gsm_Gps_project/internal/postgres"
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
		Repo:                    &postgres.Repo{},
		interfaceSettingsChange: make(chan *models.Params, 10),
	}
	go service.processPackets()
	go service.processInterfaceSettingsChange()

	return service
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
	cleanedMessage := strings.TrimPrefix(message, "SET_SETTINGS sf: ")

	parts := strings.FieldsFunc(cleanedMessage, func(r rune) bool {
		return r == ',' || r == ' '
	})

	params := models.Params{}

	if len(parts) > 0 {
		sf, err := strconv.ParseFloat(parts[0], 32)
		if err != nil {
			return err
		}
		params.Sf = float32(sf)
	}
	if len(parts) > 2 {
		tx, err := strconv.ParseFloat(parts[2], 32)
		if err != nil {
			return err
		}
		params.Tx = float32(tx)
	}
	if len(parts) > 4 {
		bw, err := strconv.ParseFloat(parts[4], 32)
		if err != nil {
			return err
		}
		params.Bandwidth = float32(bw)
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

func (s *DataService) GetMeasurements(requestID int) ([]models.Packet, error) {
	data, err := s.Repo.FindById(int32(requestID))
	if err != nil {
		log.Printf("Error getting measurements for id: %v %d", err, requestID)
		return nil, err
	}

	return data, nil
}

func (s *DataService) SendMeasurementCommand(ip, port, command string) error {
	target := ip + ":" + port
	timeout := 10 * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		log.Fatalln(fmt.Errorf("error connecting to device: %v", err))
		return err
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	log.Printf("Connected to device %v", ip)

	_, err = conn.Write([]byte(command))

	if err != nil {
		log.Fatalln(fmt.Errorf("error sending message to esp: %v", err))
		return err
	}
	log.Printf("Sent commands: %v", command)
	return nil
}

func (s *DataService) SendMeasurementsToClient(ip, port, data string) error {
	target := ip + ":" + port
	timeout := 10 * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		log.Fatalln(fmt.Errorf("error connecting to device: %v", err))
		return err
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	log.Printf("Connected to device %v", ip)
	_, err = conn.Write([]byte(data))
	if err != nil {
		log.Fatalln(fmt.Errorf("error sending message to client: %v", err))
		return err
	}
	log.Printf("Sent commands: %v", data)
	return nil
}
