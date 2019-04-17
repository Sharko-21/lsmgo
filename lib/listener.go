package lib

import (
	"fmt"
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
		<- done
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	}()

	go func() {
		defer conn.Close()
		defer wg.Done()

		var err error

		input := make([]byte, 1024 * 4)

		var inputLength int

		if inputLength, err = conn.Read(input); err != nil {
			fmt.Println(err)
			if _, err = logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err !=nil {
				fmt.Println(err)
			}
		}

		if _, err = logsFile.WriteString(GetTime(time.Now()) + " User query: " + string(input[0:inputLength]) + "\n"); err != nil {
			fmt.Println(err)
			if _, err = logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err !=nil {
				fmt.Println(err)
			}
		}

		message := "---------\n" // отправляемое сообщение

		if _, err = conn.Write([]byte(message)); err != nil {
			fmt.Println(err)
			if _, err = logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err !=nil {
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
		if _, err = logsFile.WriteString(GetTime(time.Now()) + " " + err.Error() + "\n"); err !=nil {
			fmt.Println(err)
		}
	}

	logsFile.Close()
	os.Exit(0)
}

func disassembleCommand(command string) {
	command = StandardizeSpaces(strings.Trim(command, " "))
}