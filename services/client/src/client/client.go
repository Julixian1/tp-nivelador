package client

import (
	"net"
	"time"
	"bufio"
	"os"
	"encoding/binary"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 10
const CONNECTION_ATTEMPS_DELAY_MS = 400

const (
	MsgTypeBet     byte = 1
	MsgTypeEnd     byte = 2
	MsgTypeWinners byte = 3
)

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

func createPacket(msgType byte, payload []byte) []byte {
	length := uint16(len(payload))
	packet := make([]byte, 2+1+len(payload))
	
	binary.BigEndian.PutUint16(packet[0:2], length)
	packet[2] = msgType
	copy(packet[3:], payload)
	
	return packet
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

	scanner := bufio.NewScanner(inFile)
	linesProcessed := 0

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		betData := client.config.AgencyId + "," + line
		packet := createPacket(MsgTypeBet, []byte(betData))
		if err := safe_socket.SendAll(client.conn, packet); err != nil {
			logger.Error(action, logger.Fail, "action", "send-bet", "line", linesProcessed)
			return err
		}
		linesProcessed++
	}

	if err := scanner.Err(); err != nil {
		logger.Error(action, logger.Fail, "action", "scan-file", "err", err)
		return err
	}

	endPacket := createPacket(MsgTypeEnd, []byte{})
	if err := safe_socket.SendAll(client.conn, endPacket); err != nil {
		logger.Error(action, logger.Fail, "action", "send-end-bets")
		return err
	}

	header, err := safe_socket.RecvAll(client.conn, 3)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "recv-winners-header")
		return err
	}

	payloadLen := binary.BigEndian.Uint16(header[0:2])

	var winnersPayload []byte

	if payloadLen > 0 {
		winnersPayload, err = safe_socket.RecvAll(client.conn, int(payloadLen))
		if err != nil {
			logger.Error(action, logger.Fail, "action", "recv-winners-body")
			return err
		}
	} else {
		winnersPayload = []byte{}
	}


	outFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "create-output-file", "file", client.config.OutputFile)
		return err
	}
	defer outFile.Close()

	if _, err := outFile.Write(winnersPayload); err != nil {
		logger.Error(action, logger.Fail, "action", "write-output")
		return err
	}

	logger.Info(action, logger.Success, "agency-id", client.config.AgencyId, "processed_lines", linesProcessed)
	return nil
}
