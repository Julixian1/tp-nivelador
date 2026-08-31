package client

import (
	"net"
	"time"
	"bufio"
	"os"
	"encoding/binary"
	"os/signal"
	"syscall"
	"context"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 10
const CONNECTION_ATTEMPS_DELAY_MS = 400

const (
	MsgTypeBet     byte = 1
	MsgTypeEnd     byte = 2
	MsgTypeWinners byte = 3
	MsgTypeAck 	   byte = 4
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
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

func (client *Client) sendBatchBuffer(payload []byte) error {
	if len(payload) == 0 {
        return nil
    }

	packet := createPacket(MsgTypeBet, payload)

    if err := safe_socket.SendAll(client.conn, packet); err != nil {
        logger.Error("process-bets", logger.Fail, "action", "send-bet-batch")
        return err
    }

	ackHeader, err := safe_socket.RecvAll(client.conn, 1)
	if err != nil {
		logger.Error("process-bets", logger.Fail, "action", "recv-ack")
		return err
	}

	if ackHeader[0] != MsgTypeAck {
		logger.Error("process-bets", logger.Fail, "action", "invalid-ack-type")
		return errors.New("invalid ack received")
	}
	
	return nil
}

func (client *Client) sendBetFile(action string) (int, error){
	inFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "open-input-file", "file", client.config.InputFile)
		return 0, err
	}
	defer inFile.Close()

	scanner := bufio.NewScanner(inFile)
	linesProcessed := 0
	agencyPrefix := []byte(client.config.AgencyId + ",")
	var batchPayload []byte
	itemsInBatch := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		
		batchPayload = append(batchPayload, agencyPrefix...)
        batchPayload = append(batchPayload, line...)
        batchPayload = append(batchPayload, '\n')

		itemsInBatch++
		linesProcessed++

		if itemsInBatch >= client.config.BatchSize {
            if err := client.sendBatchBuffer(batchPayload); err != nil {
                return linesProcessed, err
            }
            batchPayload = batchPayload[:0] 
            itemsInBatch = 0
        }
	}

	if err := scanner.Err(); err != nil {
		logger.Error(action, logger.Fail, "action", "scan-file", "err", err)
		return linesProcessed, err
	}

	if itemsInBatch > 0 {
        if err := client.sendBatchBuffer(batchPayload); err != nil {
            return linesProcessed, err
        }
    }
	return linesProcessed, nil
}

func (client *Client) recvAndSaveWinners(action string) error {
	header, err := safe_socket.RecvAll(client.conn, 3)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "recv-winners-header")
		return err
	}

	payloadLen := int(binary.BigEndian.Uint16(header[0:2]))

	outFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "create-output-file", "file", client.config.OutputFile)
		return err
	}
	defer outFile.Close()

	if payloadLen == 0 {
		return nil
	}

	buf := make([]byte, 4096)
	bytesRemaining := payloadLen

	for bytesRemaining > 0 {
		toRead := len(buf)
		if bytesRemaining < toRead {
			toRead = bytesRemaining
		}

		chunk, err := safe_socket.RecvAll(client.conn, toRead)
		if err != nil {
			logger.Error(action, logger.Fail, "action", "recv-winners-chunk")
			return err
		}

		if _, err := outFile.Write(chunk); err != nil {
			logger.Error(action, logger.Fail, "action", "write-output-chunk")
			return err
		}

		bytesRemaining -= len(chunk)
	}

	return nil
}

func (client *Client) Run() (err error) {
	const action = "process-bets"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	defer func() {
		if client.conn != nil {
			client.conn.Close()
		}
		if ctx.Err() != nil {
			logger.Info("graceful-shutdown", logger.Success, "agency-id", client.config.AgencyId)
			err = nil 
		}
	}()

	go func() {
		<-ctx.Done()
		logger.Info("graceful-shutdown", logger.InProgress, "agency-id", client.config.AgencyId)
		if client.conn != nil {
			client.conn.Close()
		}
	}()

	logger.Info(action, logger.InProgress, "agency-id", client.config.AgencyId)

	var linesProcessed int
	if linesProcessed, err = client.sendBetFile(action); err != nil {
		return err
	}

	endPacket := createPacket(MsgTypeEnd, []byte{})
	if err = safe_socket.SendAll(client.conn, endPacket); err != nil {
		logger.Error(action, logger.Fail, "action", "send-end-bets")
		return err
	}

	if err = client.recvAndSaveWinners(action); err != nil {
		return err
	}

	logger.Info(action, logger.Success, "agency-id", client.config.AgencyId, "processed_lines", linesProcessed)
	return nil
}
