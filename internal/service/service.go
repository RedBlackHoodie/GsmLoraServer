package service

import (
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"fmt"
)

type DataService struct {
	loraChan   chan *models.LoRaData
	gpsChan    chan *models.GPSData
	packetChan chan *models.Packet
}

func NewDataService() *DataService {
	return &DataService{
		loraChan:   make(chan *models.LoRaData, 100),
		gpsChan:    make(chan *models.GPSData, 100),
		packetChan: make(chan *models.Packet, 100),
	}
}

func (s *DataService) ProcessDataFromLora(data *models.LoRaData) error {
	ok := data == &models.LoRaData{}
	if !ok {
		fmt.Println("LoRa data empty!")
	}
	select {
	case s.loraChan <- data:
	default:
		fmt.Printf("Channel full, logging data: %+v\n", data)
	}

	return nil
}

func (s *DataService) ProcessDataFromGPS(data *models.GPSData) error {
	ok := data == &models.GPSData{}
	if !ok {
		fmt.Println("Gps data empty!")
	}
	select {
	case s.gpsChan <- data:
	default:
		fmt.Printf("Channel full, logging data: %+v\n", data)
	}

	return nil
}

func (s *DataService) ProcessPacketData(packet *models.Packet) error {
	ok := packet == &models.Packet{}
	if !ok {
		fmt.Println("Packet data empty!")
	}
	select {
	case s.packetChan <- packet:
	default:
		fmt.Printf("Channel full, logging data: %+v\n", packet)
	}

	return nil
}
