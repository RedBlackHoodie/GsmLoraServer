package app

import (
	"Lora_Esp_Gsm_Gps_project/internal/utils"
	"fmt"
)

func main() {
	hostname := "esp32-6FAD74" //move to env
	mac := "7C:9E:BD:6F:AD:74" //move to env

	ip, err := utils.ResolveEspHost(hostname, mac)

	if err != nil {
		fmt.Println("Esp not found", err)
	}
	utils.UpdateEnvFile("esp32.env", "ESP_IP", ip)

}
