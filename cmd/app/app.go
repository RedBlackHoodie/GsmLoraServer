package app

import (
	"Lora_Esp_Gsm_Gps_project/cmd/server"
	"Lora_Esp_Gsm_Gps_project/configs"
	"Lora_Esp_Gsm_Gps_project/internal/postgres"
	"Lora_Esp_Gsm_Gps_project/internal/service"
	"database/sql"
)

type App struct {
	DB          *sql.DB
	Repo        *postgres.Repo
	Server      *server.Server
	DataService *service.DataService
}

func New() *App {
	return &App{}
}

func (a *App) InitDB(cfg *configs.Config) error {
	repo := postgres.NewPostgresRepo(nil)
	db, err := repo.NewPostgresDB(*cfg)
	if err != nil {
		return err
	}

	a.DB = db
	a.Repo = repo
	return nil
}

func (a *App) InitServer(cfg *configs.Config) error {
	a.Server = server.NewServer(a.DataService)
	go func() {
		err := a.Server.StartServer(cfg.Port)
		if err != nil {
			panic(err)
		}
	}()

	return nil
}

func (a *App) InitService() {
	a.DataService = service.NewDataService()
}

func (a *App) Close() error {
	if a.DB != nil {
		return a.DB.Close()
	}
	return nil
}

//func (a *App) GetDB() *sql.DB {
//	return a.DB
//}
//
//func (a *App) GetRepo() core.Repository {
//	return a.Repo
//}
//
//func (a *App) GetService() core.Service {
//	return a.Service
//}
