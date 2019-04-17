package main

import (
	"bufio"
	"fmt"
	"lsmgo/lib"
	"net"
	"os"
	"strings"
)

var CONFIG = lib.GetApplicationConfig()

func main() {
	for {
		conn, err := net.Dial(CONFIG.COMMUNICATION_CHANNEL.NETWORK, CONFIG.COMMUNICATION_CHANNEL.ADDRES)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Print("db=# ")
		var request string

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			request = strings.Trim(request + " " + scanner.Text(), " ")

			if len(request) == 0 || request == ";" {
				request = ""
				fmt.Print("db=#")
			} else if request == "exit"{
				return
			} else if string(request[len(request) - 1:]) == ";" {
				_, err = conn.Write([]byte(string(request[:len(request) - 1])))

				if err != nil {
					fmt.Println(err)
					return
				}

				input := make([]byte, 1024 * 4)
				n, err := conn.Read(input)

				if err != nil {
					fmt.Println(err)
					return
				}

				fmt.Println(string(input[0:n]))

				err = conn.Close()
				if err != nil {
					fmt.Println(err)
					return
				}
				break
			} else {
				fmt.Print("db-# ")
			}
		}
	}
}