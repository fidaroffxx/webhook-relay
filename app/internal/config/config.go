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
	DB    *db.Config
	HTTP  *HTTPConfig
	Kafka *Kafka
}

type Kafka struct {
	Host           string `env:"KAFKA_HOST"`
	Port           string `env:"KAFKA_PORT"`
	ConnectionType string `env:"KAFKA_CONNECTION_TYPE"`
}

func NewConfig() *Config {
	return &Config{
		DB:    &db.Config{},
		HTTP:  &HTTPConfig{},
		Kafka: &Kafka{},
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

	if err := env.Parse(c.Kafka); err != nil {
		return err
	}

	return nil
}
