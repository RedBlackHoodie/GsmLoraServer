package models

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
	RSSI       int        `json:"rssi"`
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
