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
	"os/exec"
	"path/filepath"
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
	wd, err := os.Getwd()
	if err != nil {
		log.Printf("Cannot get current dir: %v", err)
	}

	scriptPath := filepath.Join(wd, "pinger.sh")

	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		log.Printf("u have to ping + arp by hand, no script found")
	}
	exec.Command("chmod", "+x", scriptPath).Run()
	cmd := exec.Command("sh", scriptPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		log.Printf("cmd.Run() failed with %s\n", err)
	}

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

	appInstance.DataService.EspInitializer()
	srv := server.NewServer(appInstance.DataService, espConnector)
	appInstance.DataService.StartProcessing()

	go func() {
		if err := srv.StartServer(dbCfg.Port); err != nil {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	select {}
}
