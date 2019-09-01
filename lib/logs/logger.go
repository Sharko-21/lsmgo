package logs

import (
	"lsmgo/lib"
	"lsmgo/lib/config"
	"os"
	"time"
)

type logger_impl struct {
	logs *os.File
}

type logger interface {
	Write(message string) error
}

func (logger *logger_impl) Write(message string) error {
	_, err := logger.logs.WriteString(lib.GetTime(time.Now()) + " " + message)
	return err
}

var Logger logger = initLogger()

func initLogger() *logger_impl {
	logger := &logger_impl{}
	logger.logs = lib.OpenFile(config.ApplicationConfig.FILES_LOCATION.LOGS_DIR_PATH + config.ApplicationConfig.FILES_LOCATION.LOGS_REQUESTS_FILE_NAME)
	return logger
}
