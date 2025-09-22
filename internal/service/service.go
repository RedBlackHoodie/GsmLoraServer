package service

import (
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"Lora_Esp_Gsm_Gps_project/internal/postgres"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
)

type DataService struct {
	packetChan        chan *models.Packet
	repo              postgres.Repo
	interfaceRequests chan *models.Params
	mu                sync.RWMutex
	isProcessing      bool
}

func NewDataService() *DataService {
	service := &DataService{
		packetChan:        make(chan *models.Packet, 100),
		repo:              postgres.Repo{},
		interfaceRequests: make(chan *models.Params, 10),
	}
	go service.processPackets()
	go service.processInterfaceRequests()

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
		err := s.repo.Save(&data)
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
			err := s.repo.Save(packet)
			if err != nil {
				log.Printf("Error saving packet during drain: %v", err)
			}
		}
	}
	log.Printf("Data channel drain completed")

	return nil
}

func (s *DataService) ProcessInterfaceRequest(message string) error {
	messageArray := strings.Split(message, ":")

	params := models.Params{}
	tx, err := strconv.ParseFloat(messageArray[0], 32)
	if err != nil {
		return err
	}
	params.Tx = float32(tx)
	sf, err := strconv.ParseFloat(messageArray[1], 32)
	if err != nil {
		return err
	}
	params.Sf = float32(sf)
	bw, err := strconv.ParseFloat(messageArray[2], 32)
	if err != nil {
		return err
	}
	params.Bandwidth = float32(bw)
	log.Printf("Got params: %+v\n", params)
	s.interfaceRequests <- &params

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
		err := s.repo.Save(packet)
		if err != nil {
			log.Printf("Error saving packet during processing: %v", err)
		}
	}
}

func (s *DataService) processInterfaceRequests() {
	config := configs.LoadEspConfig()
	for params := range s.interfaceRequests {
		err := handlers.SendParamsToDevice(config.EspIP, config.EspPort, *params)
		if err != nil {
			log.Printf("Error sending params to device: %v", err)
		} else {
			log.Printf("Params sent to device successfully: %+v", params)
		}
	}
}

func (s *DataService) GetMeasurements(requestID int) ([]models.Packet, error) {
	return nil, nil
}
