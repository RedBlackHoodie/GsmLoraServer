package models

type Destination int

const (
	Unknown Destination = iota
	Lora
	Client
)

func (d Destination) String() string {
	switch d {
	case Lora:
		return "Lora"
	case Client:
		return "Client"
	default:
		return "Unknown"
	}
}

type Coordinate struct {
	Latitude  float64
	Longitude float64
}

type GPSData struct {
	RequestId  int        `json:"request_id"`
	Coordinate Coordinate `json:"coordinate"`
	Timestamp  string     `json:"timestamp"`
	Hdop       float32    `json:"hdop"`
}

type LoRaData struct {
	RequestId int     `json:"id"`
	RSSI      int     `json:"rssi"`
	SNR       float64 `json:"snr"`
	Timestamp string  `json:"timestamp"`
}

type Packet struct {
	RequestId  int        `json:"id"`
	RSSI       float64    `json:"rssi"`
	SNRL       float64    `json:"snrl"`
	Coordinate Coordinate `json:"coordinate"`
	Hdop       float32    `json:"hdop"`
	Timestamp  string     `json:"timestamp"`
}

type Params struct {
	Bandwidth float32 `json:"bandwidth"`
	Sf        float32 `json:"sf"`
	Tx        float32 `json:"tx"`
}

type Session struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Count     int    `json:"count"`
}
