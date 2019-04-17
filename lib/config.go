package lib

import (
	"encoding/json"
	"fmt"
	"os"
)

type ConfigStruct struct {
	COMMUNICATION_CHANNEL CommunicationChannel
	FILES_LOCATION FilesLocation
}

type (
	CommunicationChannel struct {
		NETWORK string
		ADDRES  string
	}
	FilesLocation struct {
		DB_ROOT_PATH       string
		LOGS_DIR_PATH      string
		LOGS_REQUESTS_FILE_NAME string
	}
)

func GetApplicationConfig() ConfigStruct{
	var err error
	file, err := os.Open("./lib/config.json")

	if err != nil {
		fmt.Println("Err: ", err)
		fmt.Println("Will use default config")

		config := ConfigStruct{
			COMMUNICATION_CHANNEL:CommunicationChannel{"unix", "/tmp/lsmgounix"},
			FILES_LOCATION:FilesLocation{"database/", "database/logs/", "lsmgo_requests_logs"},
		}
		return config
	}

	decoder := json.NewDecoder(file)
	config := new(ConfigStruct)
	err = decoder.Decode(&config)
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}

	return *config
}