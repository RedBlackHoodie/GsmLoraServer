package main

import (
	"Lora_Esp_Gsm_Gps_project/cmd/app"
	"Lora_Esp_Gsm_Gps_project/cmd/server"
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/handlers"
	"Lora_Esp_Gsm_Gps_project/internal/utils"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	version   = "dev"
	buildTime = time.Now()
)

func main() {
	fmt.Printf("Lora ESP GSM GPS Project v%s (built %s)\n", version, buildTime)
	logFile, err := os.OpenFile("build/app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("Не удалось открыть файл для логов: %v", err)
	}

	defer func(logFile *os.File) {
		err := logFile.Close()
		if err != nil {
			log.Printf("Error closing log file: %v", err)
		}
	}(logFile)

	espCfg := configs.LoadEspConfig()
	ip, err := utils.ResolveEspHost(espCfg.EspHost, espCfg.EspMac)

	if err != nil {
		fmt.Println("Esp not found", err)
	}
	err = utils.UpdateEnvFile("esp.env", "ESP_IP", ip)
	if err != nil {
		fmt.Println("Error updating .env file:", err)
		return
	}
	app := app.New()

	dbCfg := configs.LoadConfig()
	err = app.InitDB(dbCfg)

	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}

	defer func() {
		err := app.Close()
		if err != nil {
			fmt.Println("Error closing app:", err)
		}
	}()

	handlers.Init(app)

	err = app.InitServer(dbCfg)

	server.Init(app)
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
}
