package util

import "github.com/spf13/viper"

type Config struct {
	Broker string `mapstructure:"KAFKA_BROKER"`
	InputTopic  string `mapstructure:"INPUT_TOPIC"`
	OutputTopic string `mapstructure:"OUTPUT_TOPIC"`
	MessageCount int `mapstructure:"MESSAGE_COUNT"`
}

func LoadConfig() (Config, error) {
	viper.AddConfigPath("./config")
	viper.SetConfigName("config")
	viper.SetConfigType("env")
	err := viper.ReadInConfig()
	if err!= nil {
        return Config{}, err
    }
	var config Config
	err = viper.Unmarshal(&config)
	return config, err
}

