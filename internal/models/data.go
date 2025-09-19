package models

const (
	Lora = iota + 1
	A7672g
	ESP
	Unknown
)

type Coordinate struct {
	Latitude  float64
	Longitude float64
}

type GPSData struct {
	//DeviceId   int        `json:"device_id"`
	RequestId  int        `json:"request_id"`
	Coordinate Coordinate `json:"coordinate"`
	Timestamp  string     `json:"timestamp"`
	Hdop       float32    `json:"hdop"`
	//Sat        int        `json:"sat"`
	//Fix        string     `json:"fix"`
	//SNR        float32    `json:"snr"`
}

type LoRaData struct {
	//DeviceId  int     `json:"device_id"`
	RequestId int     `json:"id"`
	RSSI      int     `json:"rssi"`
	SNR       float64 `json:"snr"`
	Timestamp string  `json:"timestamp"`
	//ErrorBits int     `json:"error_bits"`
}

type Packet struct {
	RequestId  int        `json:"id"`
	RSSI       int        `json:"rssi"`
	SNRL       float64    `json:"snrl"`
	SNRG       float32    `json:"snrg"`
	Coordinate Coordinate `json:"coordinate"`
	Hdop       float32    `json:"hdop"`
	Timestamp  string     `json:"timestamp"`
}

type Params struct {
	Bandwidth float32 `json:"bandwidth"`
	Sf        float32 `json:"sf"`
	Tx        float32 `json:"tx"`
}
