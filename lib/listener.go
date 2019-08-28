package lib

import (
	"fmt"
	"lsmgo/storage"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var CONFIG = GetApplicationConfig()

func ListenClient() {
	initializeEnvironment()

	isConnectionsCanOpen := true
	wg := &sync.WaitGroup{}
	doneConnections := make(chan struct{})

	logsFile := OpenFile(CONFIG.FILES_LOCATION.LOGS_DIR_PATH + CONFIG.FILES_LOCATION.LOGS_REQUESTS_FILE_NAME)
	if _, err := logsFile.WriteString(GetTime(time.Now()) + " Started...\n"); err != nil {
		fmt.Println(err)
	}

	channel := make(chan os.Signal, 1)
	signal.Notify(channel, syscall.SIGINT)
	go handleSIGINT(channel, *logsFile, wg, doneConnections, &isConnectionsCanOpen)

	if err := os.Remove(CONFIG.COMMUNICATION_CHANNEL.ADDRES); err != nil {
		fmt.Println(err)
	}

	listener, err := net.Listen(CONFIG.COMMUNICATION_CHANNEL.NETWORK, CONFIG.COMMUNICATION_CHANNEL.ADDRES)
	if err != nil {
		fmt.Println(err)

		if _, err := logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err != nil {
			fmt.Println(err)
		}

		return
	}

	defer os.Remove(CONFIG.COMMUNICATION_CHANNEL.ADDRES)
	defer listener.Close()
	defer logsFile.Close()

	fmt.Println("Server is listening...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		if !isConnectionsCanOpen {
			conn.Close()
			break
		}

		wg.Add(1)
		go handleConnect(conn, *logsFile, wg, doneConnections)
	}
	wg.Wait()
}

func initializeEnvironment() {
	if IsExists(CONFIG.FILES_LOCATION.DB_ROOT_PATH) == false {
		_ = os.Mkdir(CONFIG.FILES_LOCATION.DB_ROOT_PATH, 0777)
	}
	if IsExists(CONFIG.FILES_LOCATION.LOGS_DIR_PATH) == false {
		_ = os.Mkdir(CONFIG.FILES_LOCATION.LOGS_DIR_PATH, 0777)
	}
	if !IsExists(CONFIG.FILES_LOCATION.LOGS_DIR_PATH + CONFIG.FILES_LOCATION.LOGS_REQUESTS_FILE_NAME) {
		file, _ := os.Create(CONFIG.FILES_LOCATION.LOGS_DIR_PATH + CONFIG.FILES_LOCATION.LOGS_REQUESTS_FILE_NAME)
		file.Close()
	}
}

func handleConnect(conn net.Conn, logsFile os.File, wg *sync.WaitGroup, done chan struct{}) {
	go func() {
		<-done
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	}()

	go func() {
		defer conn.Close()
		defer wg.Done()

		var err error

		input := make([]byte, 1024*4)

		var inputLength int

		if inputLength, err = conn.Read(input); err != nil {
			fmt.Println(err)
			if _, err = logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err != nil {
				fmt.Println(err)
			}
		}

		res := disassembleCommand(string(input[0:inputLength]))
		if _, err = logsFile.WriteString(GetTime(time.Now()) + " User query: " + string(input[0:inputLength]) + "\n"); err != nil {
			fmt.Println(err)
			if _, err = logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err != nil {
				fmt.Println(err)
			}
		}

		message := "---------\n" // отправляемое сообщение
		message += res + "\n"

		if _, err = conn.Write([]byte(message)); err != nil {
			fmt.Println(err)
			if _, err = logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err != nil {
				fmt.Println(err)
			}
		}
	}()
}

func handleSIGINT(channel chan os.Signal, logsFile os.File, wg *sync.WaitGroup, doneConnections chan struct{}, isConnectionCanOpen *bool) {
	<-channel
	var err error

	*isConnectionCanOpen = false

	close(doneConnections)
	wg.Wait()

	if _, err = logsFile.WriteString(GetTime(time.Now()) + " Shutdown: SIGINT\n"); err != nil {
		fmt.Println(err)
		if _, err = logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err != nil {
			fmt.Println(err)
		}
	}

	logsFile.Close()
	os.Exit(0)
}

func disassembleCommand(command string) string {
	command = StandardizeSpaces(strings.Trim(command, " "))
	commands := strings.Split(command, " ")

	switch strings.ToLower(commands[0]) {
	case "select":
		return disassembleSelectCommand(commands)
	case "insert":
		return disassembleInsertCommand(commands)
	case "update":
		return disassembleUpdateCommand(commands)
	case "delete":
		return disassembleDeleteCommand(commands)
	default:
		return "wrong command"
	}
}

func disassembleSelectCommand(commands []string) string {
	var res string
	if len(commands) == 2 {
		res = storage.Storage.FindByKey(commands[1])
	} else if len(commands) == 3 {
		if strings.ToLower(commands[2]) != "value" {
			return "Wrong select command"
		}
		res = storage.Storage.FindByValue(commands[1], false)
	} else if len(commands) == 4 {
		if strings.ToLower(commands[2]) != "like" && strings.ToLower(commands[3]) != "value" {
			return "Wrong select command"
		}
		res = storage.Storage.FindByValue(commands[1], true)
	} else {
		return "Wrong select command"
	}
	return res
}

func disassembleInsertCommand(commands []string) string {
	if len(commands) != 4 || strings.ToLower(commands[2]) != "value" {
		return "Wrong insert command"
	}
	storage.Storage.Insert(commands[1], commands[3])
	return "Inserted!"
}

func disassembleUpdateCommand(commands []string) string {
	if len(commands) != 4 || strings.ToLower(commands[2]) != "value" {
		return "Wrong update command"
	}
	return storage.Storage.Update(commands[1], commands[3])
}

func disassembleDeleteCommand(commands []string) string {
	if len(commands) != 2 {
		return "Wrong update command"
	}
	return storage.Storage.Update(commands[1], "/_lsmgo_deleted/")
}
