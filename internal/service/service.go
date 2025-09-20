package service

import (
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"Lora_Esp_Gsm_Gps_project/internal/postgres"
	"fmt"
	"log"
)

type DataService struct {
	packetChan chan *models.Packet
	repo       postgres.Repo
}

func NewDataService() *DataService {
	return &DataService{
		packetChan: make(chan *models.Packet, 100),
		repo:       postgres.Repo{},
	}
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
		log.Fatal("Unexpected error while parsing packet: ", err)
	}

	select {
	case s.packetChan <- &data:
	default:
		fmt.Printf("Channel full, saving data and starting channel drain: %+v\n", data)
		s.repo.Save(&data)
		_ = s.DrainChannel()
	}
	log.Printf("Data channel usage :%f", s.GetChannelUsage())

	return nil
}

func (s *DataService) DrainChannel() error {
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
