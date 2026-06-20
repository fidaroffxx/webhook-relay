package db

type Config struct {
	Driver   string `env:"DB_DRIVER"`
	User     string `env:"DB_USER"`
	Password string `env:"DB_PASSWORD"`
	Host     string `env:"DB_HOST"`
	Port     uint16 `env:"DB_PORT"`
	Name     string `env:"DB_NAME"`
}

func (c *Config) GetDriver() string {
	return c.Driver
}

func (c *Config) GetUser() string {
	return c.User
}

func (c *Config) GetPassword() string {
	return c.Password
}

func (c *Config) GetHost() string {
	return c.Host
}

func (c *Config) GetPort() uint16 {
	return c.Port
}

func (c *Config) GetName() string {
	return c.Name
}
