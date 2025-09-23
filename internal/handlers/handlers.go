package handlers

import (
	"Lora_Esp_Gsm_Gps_project/internal/core"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
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
	p.Timestamp = time.Now()
	p.Data = &lora

	return nil
}

func (p *PacketParser) ParsePacketData(response string) (models.Packet, error) {
	rawData := PacketParser{}
	if err := json.Unmarshal([]byte(response), &rawData); err != nil {
		p.RequestId = rawData.RequestId
		p.Checksum = rawData.Checksum
	}
	packet := models.Packet{}
	packet.RequestId = rawData.RequestId
	packet.RSSI = rawData.Data.RSSI
	packet.SNRL = rawData.Data.SNRL
	packet.Coordinate = rawData.Data.Coordinate
	packet.Hdop = rawData.Data.Hdop
	packet.Timestamp = rawData.Data.Timestamp
	p.Timestamp = time.Now()
	p.Data = &packet

	return packet, nil
}

func SendParamsToDevice(ip string, port string, config models.Params) error {
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

	message := fmt.Sprintf("sf: %f, tx: %f, bw: %f", config.Sf, config.Tx, config.Bandwidth)

	_, err = conn.Write([]byte(message))

	if err != nil {
		log.Fatalln(fmt.Errorf("error sending message: %v", err))
		return err
	}
	log.Printf("Sent params: %v", message)
	return nil
}

type Message interface {
	Type() string
}

type SetSettingsMessage struct {
	Params models.Params
}

type GetDataMessage struct {
	Data []models.Packet
}
type StartMeasurementMessage struct {
	RequestId int
}

type StopMeasurementMessage struct {
	RequestId int
}

type UnknownMessageMessage struct{}

func (m SetSettingsMessage) Type() string      { return "SET_SETTINGS" }
func (m GetDataMessage) Type() string          { return "GET_MEASUREMENT" }
func (m StartMeasurementMessage) Type() string { return "START_MEASUREMENT" }
func (m StopMeasurementMessage) Type() string  { return "STOP_MEASUREMENT" }
func (m UnknownMessageMessage) Type() string   { return "UNKNOWN" }

type GetMessage struct {
	What string
}

func ParseClientMessage(h core.MeasurementHandler, message string) (Message, error) {
	if strings.HasPrefix(message, "SET_SETTINGS") {
		par := models.Params{}
		_, err := fmt.Sscanf(message, "SET_SETTINGS: SF=%f, TX=%f, BW=%f", &par.Sf, &par.Tx, &par.Bandwidth)
		if err != nil {
			log.Printf("error parsing params: %v", err)
		}
		return SetSettingsMessage{Params: par}, nil

	} else if strings.HasPrefix(message, "GET_MEASUREMENTS") {
		requestId := 0
		_, err := fmt.Sscanf(message, "REQUEST_ID=%d", &requestId)
		if err != nil {
			log.Printf("error parsing get request: %v", err)
		}
		data, err := h.GetMeasurements(requestId)
		if err != nil {
			return nil, err
		}

		return GetDataMessage{Data: data}, nil
	} else if strings.HasPrefix(message, "START_MEASUREMENTS") {
		requestId := 0
		_, err := fmt.Sscanf(message, "REQUEST_ID=%d", &requestId)
		if err != nil {
			log.Printf("error parsing get request: %v", err)
		}
		return StartMeasurementMessage{RequestId: requestId}, nil
	} else if strings.HasPrefix(message, "STOP_MEASUREMENTS") {
		requestId := 0
		_, err := fmt.Sscanf(message, "REQUEST_ID=%d", &requestId)
		if err != nil {
			log.Printf("error parsing get request: %v", err)
		}
		return StopMeasurementMessage{RequestId: requestId}, nil
	}

	return UnknownMessageMessage{}, nil
}
