package handlers

import (
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"github.com/adrianmo/go-nmea"
	"log"
	"time"
)

type GpsParser struct {
	Data      *models.GPSData
	Timestamp time.Time
}

type LoraParser struct {
	Data      *models.LoRaData
	Timestamp time.Time
}

func (p *GpsParser) ParseGpsData(response string) error {

	data, err := nmea.Parse(response)
	if err != nil {
		log.Fatal(err)
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

}
