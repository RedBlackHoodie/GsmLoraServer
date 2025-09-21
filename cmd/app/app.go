package app

import (
	"Lora_Esp_Gsm_Gps_project/cmd/server"
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/postgres"
	"Lora_Esp_Gsm_Gps_project/internal/utils"
	"database/sql"
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
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Не удалось открыть файл для логов: %v", err)
	}
	defer logFile.Close()
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
	repo := postgres.NewPostgresRepo(nil)
	dbCfg := configs.LoadConfig()
	db, err := repo.NewPostgresDB(*dbCfg)

	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("Error closing database:", err)
		}
	}(db)

	mainServer := server.ServerInst
	err = mainServer.StartServer()
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
}
