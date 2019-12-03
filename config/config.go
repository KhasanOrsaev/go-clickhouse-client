package config

import (
	"github.com/go-errors/errors"
	"gopkg.in/yaml.v2"
)

type Configuration struct {
	Host string `yaml:"host"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
	SSLVerify bool `yaml:"ssl_verify"`
	Queries map[string]string `yaml:"queries"`
	Workers int `yaml:"workers"`
	Bulk int `yaml:"bulk"`
}

var conf Configuration

func GetConfig() *Configuration {
	return &conf
}

func InitConfig(f []byte) error {
	err:= yaml.Unmarshal(f, &conf)
	if err != nil {
		return errors.Wrap(err, -1)
	}
	return nil
}
