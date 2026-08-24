package client

import (
	"net"
	"time"
	"bufio"
	"os"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const ECHO_CLIENT_BUFFER_SIZE = 512
const ECHO_CLIENT_MESSAGE_AMOUNT = 3
const ECHO_CLIENT_MESSAGE_DELAY_MS = 1000

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	const action = "process-bets"
	defer client.conn.Close()

	logger.Info(action, logger.InProgress, "agency-id", client.config.AgencyId)

	inFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "open-input-file", "file", client.config.InputFile)
		return err
	}
	defer inFile.Close()

	outFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "create-output-file", "file", client.config.OutputFile)
		return err
	}
	defer outFile.Close()

	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)

	linesProcessed := 0

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		if err := safe_socket.SendAll(client.conn, []byte(line)); err != nil {
			logger.Error(action, logger.Fail, "action", "send-bet", "line", linesProcessed)
			return err
		}

		respBytes, err := safe_socket.RecvAll(client.conn, ECHO_CLIENT_BUFFER_SIZE)
		if err != nil {
			logger.Error(action, logger.Fail, "action", "recv-response", "line", linesProcessed)
			return err
		}

		_, err = writer.WriteString(string(respBytes) + "\n")
		if err != nil {
			logger.Error(action, logger.Fail, "action", "write-output", "line", linesProcessed)
			return err
		}

		linesProcessed++
	}

	if err := scanner.Err(); err != nil {
		logger.Error(action, logger.Fail, "action", "scan-file", "err", err)
		return err
	}

	if err := writer.Flush(); err != nil {
		logger.Error(action, logger.Fail, "action", "flush-writer", "err", err)
		return err
	}

	logger.Info(action, logger.Success, "agency-id", client.config.AgencyId, "processed_lines", linesProcessed)
	return nil
}
