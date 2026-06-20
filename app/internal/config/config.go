package config

import (
	"github.com/fidaroffxx/webhook-relay/internal/db"

	"github.com/caarlos0/env/v11"
	"github.com/lpernett/godotenv"
)

const (
	envPath = "../.env"
)

type HTTPConfig struct {
	Protocol string `env:"APP_PROTOCOL"`
	Host     string `env:"APP_HOST"`
	Port     string `env:"APP_PORT"`
}

type Config struct {
	DB   *db.Config
	HTTP *HTTPConfig
}

func NewConfig() *Config {
	return &Config{
		DB:   &db.Config{},
		HTTP: &HTTPConfig{},
	}
}

func (c *Config) Load() error {
	if err := godotenv.Load(envPath); err != nil {
		return err
	}

	if err := env.Parse(c.DB); err != nil {
		return err
	}

	if err := env.Parse(c.HTTP); err != nil {
		return err
	}

	return nil
}
