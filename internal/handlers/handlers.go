package handlers

import (
	"Lora_Esp_Gsm_Gps_project/internal/core"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
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
	rawData := models.Packet{}
	if err := json.Unmarshal([]byte(response), &rawData); err != nil {
	}
	packet := models.Packet{}
	packet.RequestId = rawData.RequestId
	packet.RSSI = rawData.RSSI
	packet.SNRL = rawData.SNRL
	packet.Coordinate = rawData.Coordinate
	packet.Hdop = rawData.Hdop
	packet.Timestamp = rawData.Timestamp

	return packet, nil
}

type Message interface {
	Type() string
}

type SetSettingsMessage struct {
	Params models.Params
}

type StartMeasurementMessage struct {
	SessionId int
}

type StopMeasurementMessage struct {
	RequestId int
}

type GetMeasurementSessionsMessage struct{}

type AddSessionMessage struct {
	Session models.Session
}

type RemoveSessionMessage struct {
	SessionId int
}

type IncomingMeasurementMessage struct {
	Data      models.Packet
	sessionId int
}

type EspMessage struct{}

type AckMessage struct{}

type ErrMessage struct {
	error string
}

type InitialEspConnectionMessage struct{}

type MasterStatusMessage struct {
	Status string
}

type SlaveStatusMessage struct {
	Status string
}
type UnknownMessage struct{}

func (m SetSettingsMessage) Type() string            { return "SET_SETTINGS" }
func (m StartMeasurementMessage) Type() string       { return "START_MEASUREMENT" }
func (m StopMeasurementMessage) Type() string        { return "STOP_MEASUREMENT" }
func (m GetMeasurementSessionsMessage) Type() string { return "GET_MEASUREMENT_SESSIONS" }
func (m AddSessionMessage) Type() string             { return "ADD_SESSION" }
func (m RemoveSessionMessage) Type() string          { return "REMOVE_SESSION" }
func (m UnknownMessage) Type() string                { return "UNKNOWN" }
func (m EspMessage) Type() string                    { return "ESP_IDENTIFY" }
func (m InitialEspConnectionMessage) Type() string   { return "INITIAL_ESP_CONNECTION" }
func (m IncomingMeasurementMessage) Type() string    { return "ESP_MEASUREMENT" }
func (m AckMessage) Type() string                    { return "ACK" }
func (m ErrMessage) Type() string                    { return "ERROR" }
func (m MasterStatusMessage) Type() string           { return "MASTER_STATUS" }
func (m SlaveStatusMessage) Type() string            { return "SLAVE_STATUS" }

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

	} else if strings.HasPrefix(message, "START_MEASUREMENT") {
		cleaned := strings.TrimPrefix(message, "START_MEASUREMENT: ")
		sessionId, err := strconv.Atoi(strings.TrimSpace(cleaned))
		if err != nil {
			log.Printf("error parsing get request: %v", err)
		}
		return StartMeasurementMessage{SessionId: sessionId}, nil

	} else if strings.HasPrefix(message, "GET_MEASUREMENT_SESSIONS") {
		return GetMeasurementSessionsMessage{}, nil

	} else if strings.HasPrefix(message, "ADD_SESSION") {
		sessionStr := strings.TrimPrefix(message, "ADD_SESSION: ")
		session, err := ParseSession(sessionStr)
		if err != nil {
			log.Printf("error parsing ADD_SESSION: %v", err)
			return nil, err
		}
		return AddSessionMessage{Session: session}, nil

	} else if strings.HasPrefix(message, "REMOVE_SESSION") {
		sessionId, err := ParseRemoveSessionMessage(message)
		if err != nil {
			log.Printf("error parsing REMOVE_SESSION: %v", err)
			return nil, err
		}
		return RemoveSessionMessage{SessionId: sessionId}, nil

	} else if strings.HasPrefix(message, "IDENTIFY") {
		return EspMessage{}, nil

	} else if strings.HasPrefix(message, "MEASUREMENT:") {
		cleaned := strings.TrimPrefix(message, "MEASUREMENT:")
		packetParser := PacketParser{}
		data, err := packetParser.ParsePacketData(cleaned)
		if err != nil {
			return IncomingMeasurementMessage{}, fmt.Errorf("error parsing incoming measurement: %w", err)
		}
		incoming := IncomingMeasurementMessage{
			Data:      data,
			sessionId: 0,
		}
		return incoming, nil

	} else if strings.Contains(message, "ACK") {
		return AckMessage{}, nil

	} else if strings.Contains(message, "ERROR") {
		return ErrMessage{error: message}, nil

	} else if strings.Contains(message, "MASTER_STATUS") {
		status := strings.TrimPrefix(message, "MASTER_STATUS ")

		return MasterStatusMessage{status}, nil

	} else if strings.Contains(message, "SLAVE_STATUS") {
		status := strings.TrimPrefix(message, "SLAVE_STATUS ")

		return SlaveStatusMessage{status}, nil
	}

	return UnknownMessage{}, nil
}

func ParseSession(data string) (models.Session, error) {
	cleaned := strings.TrimPrefix(data, "ADD_SESSION: ")
	cleaned = strings.Trim(cleaned, "[]")

	parts := strings.FieldsFunc(cleaned, func(r rune) bool {
		return r == ',' || r == ' '
	})

	var cleanParts []string
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			cleanParts = append(cleanParts, strings.TrimSpace(part))
		}
	}
	log.Printf("Raw data: %s", data)
	log.Printf("Cleaned: %s", cleaned)
	log.Printf("Parts: %v, len: %d", parts, len(parts))

	var session models.Session
	if len(cleanParts) != 5 {
		log.Printf("Error parsing session: %v, got len: %v", data, len(cleanParts))
		return models.Session{}, errors.New("invalid session format")
	}

	sessionID, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		log.Printf("Error parsing session: %v", data)
		return models.Session{}, errors.New("invalid session format")
	}
	session.Id = sessionID
	session.Name = strings.TrimSpace(parts[1])
	session.StartTime = strings.TrimSpace(parts[2])
	session.EndTime = strings.TrimSpace(parts[3])
	count, err := strconv.Atoi(strings.TrimSpace(parts[4]))
	if err != nil {
		return session, fmt.Errorf("invalid points: %w", err)
	}
	session.Count = count
	return session, nil
}

func ParseRemoveSessionMessage(message string) (int, error) {
	cleaned := strings.TrimPrefix(message, "REMOVE_SESSION: ")

	sessionID, err := strconv.Atoi(strings.TrimSpace(cleaned))
	if err != nil {
		return 0, fmt.Errorf("invalid session ID: %w", err)
	}

	return sessionID, nil
}

func ParseTime(timeStr string) (time.Time, error) {
	layout := "2006-01-02T15:04:05 +0000 UTC m=+1101.042160876"
	parsedTime, err := time.Parse(layout, timeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time format: %w", err)
	}
	return parsedTime, nil
}

func ParseIncomingMeasurement(message string) (IncomingMeasurementMessage, error) {
	cleaned := strings.TrimPrefix(message, "MEASUREMENT: ")
	packetParser := PacketParser{}
	_, err := packetParser.ParsePacketData(cleaned)
	if err != nil {
		return IncomingMeasurementMessage{}, fmt.Errorf("error parsing incoming measurement: %w", err)
	}
	incoming := IncomingMeasurementMessage{
		Data:      *packetParser.Data,
		sessionId: 0,
	}
	return incoming, nil
}
