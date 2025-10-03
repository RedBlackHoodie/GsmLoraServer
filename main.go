package main

import (
	"Lora_Esp_Gsm_Gps_project/cmd/app"
	"Lora_Esp_Gsm_Gps_project/cmd/server"
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/esp"
	"Lora_Esp_Gsm_Gps_project/internal/utils"
	"io"
	"log"
	"os"
	"time"
)

var (
	version   = "dev"
	buildTime = time.Now().String()
)

func main() {
	log.Printf("Lora ESP GSM GPS Project v.%s (built %s)\n", version, buildTime)
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("Не удалось открыть файл для логов: %v", err)
	} else {
		multiWriter := io.MultiWriter(os.Stdout, logFile)
		log.SetOutput(multiWriter)
	}

	defer func(logFile *os.File) {
		err := logFile.Close()
		if err != nil {
			log.Printf("Error closing log file: %v", err)
		}
	}(logFile)

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	espCfg := configs.LoadEspConfig()
	ip, err := utils.ResolveEspHost(espCfg.EspHost, espCfg.EspMac)

	if err != nil {
		log.Println("Esp not found", err)
	}
	err = utils.UpdateEnvFile("esp.env", "ESP_IP", ip)
	if err != nil {
		log.Println("Error updating esp.env file:", err)
		return
	}
	log.Printf("ESP32 IP: %s\n", ip)
	espConnector := esp.NewESPConnector()
	espConnector.IP = ip
	espConnector.Port = espCfg.EspPort

	appInstance := app.New()

	dbCfg := configs.LoadConfig()
	appInstance.EspConnector = espConnector
	appInstance.InitService(espConnector)

	err = appInstance.InitDB(dbCfg)
	if err != nil {
		log.Println("Error connecting to database:", err)
		return
	}
	defer func(appInstance *app.App) {
		err := appInstance.Close()
		if err != nil {
			log.Println("Error closing app:", err)
		}
	}(appInstance)

	appInstance.DataService.StartProcessing()
	srv := server.NewServer(appInstance.DataService, espConnector)

	go func() {
		if err := srv.StartServer(dbCfg.Port); err != nil {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	select {}
}
