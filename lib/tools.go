package lib

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func GetTime(time time.Time) string{
	return time.Format("2006.01.02 15:04:05")
}

func IsExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil { return true }
	if os.IsNotExist(err) { return false }
	return true
}

func OpenFile (path string) *os.File {
	var file *os.File
	var err error

	if IsExists(path) {
		file, err = os.OpenFile(path, os.O_APPEND | os.O_RDWR, 0666)

		if err != nil {
			fmt.Println(err)
		}
	} else {
		file, err = os.Create(path)

		if err != nil {
			fmt.Println(err)
		}
	}

	return file
}

func StandardizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}