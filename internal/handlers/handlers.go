package handlers

import (
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"encoding/json"
	"log"
	"time"

	"github.com/adrianmo/go-nmea"
)

type GpsParser struct {
	Data      *models.GPSData
	Timestamp time.Time
	RequestId int
	Checksum  *string
}

type gpsResponse struct {
	RequestId int     `json:"request_id"`
	GGA       string  `json:"gga"`
	Checksum  *string `json:"checksum"`
}

type loraResponse struct {
	RequestId int     `json:"id"`
	RSSI      int     `json:"rssi"`
	SNR       float64 `json:"snr"`
	ErrorBits int     `json:"error_bits"`
	Checksum  *string `json:"checksum"`
}

type LoraParser struct {
	Data      *models.LoRaData
	Timestamp time.Time
	RequestId int
	Checksum  *string
}

type PacketParser struct {
	Data      *models.Packet
	Timestamp time.Time
	RequestId int
	Checksum  *string
}

func (p *GpsParser) ParseGpsData(response string) error {
	rawData := gpsResponse{}
	if err := json.Unmarshal([]byte(response), &rawData); err != nil {
		p.RequestId = rawData.RequestId
		p.Checksum = rawData.Checksum
	}

	data, err := nmea.Parse(rawData.GGA)
	if err != nil {
		log.Fatalf("NMEA Gps data parsing failed: %v", err)
		return err
	}
	gps := models.GPSData{}
	if data.DataType() == nmea.TypeGGA {
		gga := data.(nmea.GGA)
		gps.Timestamp = gga.Time.String()
		coord := models.Coordinate{}
		coord.Latitude = gga.Latitude
		coord.Longitude = gga.Longitude
		gps.Coordinate = coord
		gps.Hdop = float32(gga.HDOP)
	}
	p.Timestamp = time.Now()
	p.Data = &gps
	return nil
}

func (p *LoraParser) ParseLoraData(response string) error {
	rawData := loraResponse{}
	if err := json.Unmarshal([]byte(response), &rawData); err != nil {
		p.RequestId = rawData.RequestId
		p.Checksum = rawData.Checksum
	}
	lora := models.LoRaData{}
	lora.RequestId = rawData.RequestId
	lora.RSSI = rawData.RSSI
	lora.SNR = rawData.SNR
	//lora.ErrorBits = rawData.ErrorBits
	p.Timestamp = time.Now()
	p.Data = &lora

	return nil
}

func (p *PacketParser) ParsePacketData(response string) error {
	rawData := PacketParser{}
	if err := json.Unmarshal([]byte(response), &rawData); err != nil {
		p.RequestId = rawData.RequestId
		p.Checksum = rawData.Checksum
	}
	packet := models.Packet{}
	packet.RequestId = rawData.RequestId
	packet.RSSI = rawData.Data.RSSI
	packet.SNRL = rawData.Data.SNRL
	packet.SNRG = rawData.Data.SNRG
	packet.Coordinate = rawData.Data.Coordinate
	packet.Hdop = rawData.Data.Hdop
	packet.Timestamp = rawData.Data.Timestamp
	p.Timestamp = time.Now()
	p.Data = &packet

	return nil
}
