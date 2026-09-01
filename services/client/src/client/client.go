package client

import (
	"net"
	"time"
	"bufio"
	"os"
	"fmt"
	"encoding/binary"
	"os/signal"
	"strconv"
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

func appendBetBinary(dst []byte, agencyID uint16, line []byte) ([]byte, error) {
	var c1, c2, c3, c4 int
	found := 0
	for i, b := range line {
		if b == ',' {
			switch found {
			case 0: c1 = i
			case 1: c2 = i
			case 2: c3 = i
			case 3: c4 = i
			}
			found++
		}
	}
	if found < 4 {
		return dst, errors.New("formato invalido")
	}

	nombre := line[:c1]
	apellido := line[c1+1 : c2]
	dniBytes := line[c2+1 : c3]
	fechaBytes := line[c3+1 : c4]
	numeroBytes := line[c4+1:]

	dni, err := strconv.ParseUint(string(dniBytes), 10, 64)
	if err != nil {
		return dst, err
	}

	numero, err := strconv.ParseUint(string(numeroBytes), 10, 64)
	if err != nil {
		return dst, err
	}

	var anio, mes, dia uint64
	if len(fechaBytes) >= 10 && fechaBytes[4] == '-' && fechaBytes[7] == '-' {
		anio, _ = strconv.ParseUint(string(fechaBytes[0:4]), 10, 64)
		mes, _ = strconv.ParseUint(string(fechaBytes[5:7]), 10, 64)
		dia, _ = strconv.ParseUint(string(fechaBytes[8:10]), 10, 64)
	}

	dst = binary.BigEndian.AppendUint16(dst, agencyID)
	dst = append(dst, byte(len(nombre)))
	dst = append(dst, nombre...)
	dst = append(dst, byte(len(apellido)))
	dst = append(dst, apellido...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(dni))

	dst = binary.BigEndian.AppendUint16(dst, uint16(anio))
	dst = append(dst, byte(mes), byte(dia))

	dst = binary.BigEndian.AppendUint16(dst, uint16(numero))
	return dst, nil
}

func (client *Client) sendBatchPayload(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	var header [3]byte
	binary.BigEndian.PutUint16(header[0:2], uint16(len(payload)))
	header[2] = MsgTypeBet

	if err := safe_socket.SendAll(client.conn, header[:]); err != nil {
		logger.Error("process-bets", logger.Fail, "action", "send-bet-batch")
		return err
	}

	if err := safe_socket.SendAll(client.conn, payload); err != nil {
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
		
		batchPayload, err = appendBetBinary(batchPayload, agencyID, line)
		if err != nil {
			logger.Error(action, logger.Fail, "action", "append-bet-binary", "err", err)
			return linesProcessed, err
		}

		itemsInBatch++
		linesProcessed++

		if itemsInBatch >= client.config.BatchSize {
			if err := client.sendBatchPayload(batchPayload); err != nil {
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
		if err := client.sendBatchPayload(batchPayload); err != nil {
			return linesProcessed, err
		}
	}
	return linesProcessed, nil
}

func readWinnerFromSocket(conn net.Conn) (string, int, error) {
	bytesRead := 0

	bufLen, err := safe_socket.RecvAll(conn, 1)
	if err != nil {
		return "", 0, err
	}
	nombreLen := int(bufLen[0])
	bytesRead += 1

	bufNombre, err := safe_socket.RecvAll(conn, nombreLen)
	if err != nil {
		return "", 0, err
	}
	nombre := string(bufNombre)
	bytesRead += nombreLen

	bufLen, err = safe_socket.RecvAll(conn, 1)
	if err != nil {
		return "", 0, err
	}
	apellidoLen := int(bufLen[0])
	bytesRead += 1

	bufApellido, err := safe_socket.RecvAll(conn, apellidoLen)
	if err != nil {
		return "", 0, err
	}
	apellido := string(bufApellido)
	bytesRead += apellidoLen

	bufDoc, err := safe_socket.RecvAll(conn, 4)
	if err != nil {
		return "", 0, err
	}
	documento := binary.BigEndian.Uint32(bufDoc)
	bytesRead += 4

	bufFecha, err := safe_socket.RecvAll(conn, 4)
	if err != nil {
		return "", 0, err
	}
	anio := binary.BigEndian.Uint16(bufFecha[0:2])
	mes := bufFecha[2]
	dia := bufFecha[3]
	bytesRead += 4

	bufNum, err := safe_socket.RecvAll(conn, 2)
	if err != nil {
		return "", 0, err
	}
	numero := binary.BigEndian.Uint16(bufNum)
	bytesRead += 2

	line := fmt.Sprintf("%s,%s,%d,%04d-%02d-%02d,%d\n", nombre, apellido, documento, anio, mes, dia, numero)
	return line, bytesRead, nil
}

func (client *Client) recvAndSaveWinners(action string) error {

	outFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error(action, logger.Fail, "action", "create-output-file", "file", client.config.OutputFile)
		return err
	}
	defer outFile.Close()

	for {
		header, err := safe_socket.RecvAll(client.conn, 3)
		if err != nil {
			logger.Error(action, logger.Fail, "action", "recv-winners-header")
			return err
		}

		payloadLen := int(binary.BigEndian.Uint16(header[0:2]))

		if payloadLen == 0 {
            break
        }

		bytesReadInChunk := 0
		for bytesReadInChunk < payloadLen {
			line, bytesRead, err := readWinnerFromSocket(client.conn)
			if err != nil {
				logger.Error(action, logger.Fail, "action", "read-winner-item")
				return err
			}

			if _, err := outFile.WriteString(line); err != nil {
				logger.Error(action, logger.Fail, "action", "write-output-line")
				return err
			}

			bytesReadInChunk += bytesRead
		}

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