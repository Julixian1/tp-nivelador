package client

import (
	"net"
	"time"
	"bufio"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"context"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 10
const CONNECTION_ATTEMPS_DELAY_MS = 400

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

func (client *Client) sendBetFile(action string) (int, error){
	inFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "open-input-file", "file", client.config.InputFile)
		return 0, err
	}
	defer inFile.Close()

	scanner := bufio.NewScanner(inFile)
	linesProcessed := 0
	agencyIDParsed, err := strconv.ParseUint(client.config.AgencyId, 10, 16)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "parse-agency-id", "err", err)
		return 0, err
	}
	agencyID := uint16(agencyIDParsed)
	var batchPayload []byte
	itemsInBatch := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		
		batchPayload, err = protocol.AppendBetBinary(batchPayload, agencyID, line)
		if err != nil {
			logger.Error(action, logger.Fail, "action", "append-bet-binary", "err", err)
			return linesProcessed, err
		}

		itemsInBatch++
		linesProcessed++

		if itemsInBatch >= client.config.BatchSize {
			if err := protocol.SendBatchPayload(client.conn, batchPayload); err != nil {
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
		if err := protocol.SendBatchPayload(client.conn, batchPayload); err != nil {
			return linesProcessed, err
		}
	}
	return linesProcessed, nil
}

func (client *Client) recvAndSaveWinners(action string) error {
	outFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "create-output-file", "file", client.config.OutputFile)
		return err
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)

	for {
		payloadLen, err := protocol.ReadWinnersChunkHeader(client.conn)
		if err != nil {
			logger.Error(action, logger.Fail, "action", "recv-winners-header")
			return err
		}

		if payloadLen == 0 {
            break
        }

		chunkPayload, err := protocol.ReadWinnersChunkPayload(client.conn, payloadLen)
        if err != nil {
            logger.Error(action, logger.Fail, "action", "recv-winners-chunk")
            return err
        }

		offset := 0
		for offset < payloadLen {
            line, newOffset, err := protocol.ParseWinnerFromBytes(chunkPayload, offset)
            if err != nil {
                logger.Error(action, logger.Fail, "action", "parse-winner-item")
                return err
            }

            if _, err := writer.WriteString(line); err != nil {
                logger.Error(action, logger.Fail, "action", "write-output-line")
                return err
            }

            offset = newOffset
        }

	}

	if err := writer.Flush(); err != nil {
        logger.Error(action, logger.Fail, "action", "flush-output-file")
        return err
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

	if err = protocol.SendEndMsg(client.conn); err != nil {
		logger.Error(action, logger.Fail, "action", "send-end-bets")
		return err
	}

	if err = client.recvAndSaveWinners(action); err != nil {
		return err
	}

	logger.Info(action, logger.Success, "agency-id", client.config.AgencyId, "processed_lines", linesProcessed)
	return nil
}