package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	MsgTypeBet     byte = 1
	MsgTypeEnd     byte = 2
	MsgTypeWinners byte = 3
	MsgTypeAck     byte = 4
)


func createPacket(msgType byte, payload []byte) []byte {
	length := uint16(len(payload))
	packet := make([]byte, 2+1+len(payload))
	
	binary.BigEndian.PutUint16(packet[0:2], length)
	packet[2] = msgType
	copy(packet[3:], payload)
	
	return packet
}

func AppendBetBinary(dst []byte, agencyID uint16, line []byte) ([]byte, error) {
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

func SendBatchPayload(rw io.ReadWriter, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	var header [3]byte
	binary.BigEndian.PutUint16(header[0:2], uint16(len(payload)))
	header[2] = MsgTypeBet

	if err := safe_socket.SendAll(rw, header[:]); err != nil {
		return err
	}

	if err := safe_socket.SendAll(rw, payload); err != nil {
		return err
	}

	ackHeader, err := safe_socket.RecvAll(rw, 1)
	if err != nil {
		return err
	}

	if ackHeader[0] != MsgTypeAck {
		return errors.New("invalid ack received")
	}

	return nil
}

func SendEndMsg(w io.Writer) error {
	endPacket := createPacket(MsgTypeEnd, []byte{})
	return safe_socket.SendAll(w, endPacket)
}

func ReadWinnersChunkHeader(r io.Reader) (int, error) {
	header, err := safe_socket.RecvAll(r, 3)
	if err != nil {
		return 0, err
	}

	payloadLen := int(binary.BigEndian.Uint16(header[0:2]))
	return payloadLen, nil
}


func ReadWinnerFromSocket(r io.Reader) (string, int, error) {
	bytesRead := 0

	bufLen, err := safe_socket.RecvAll(r, 1)
	if err != nil {
		return "", 0, err
	}
	nombreLen := int(bufLen[0])
	bytesRead += 1

	bufNombre, err := safe_socket.RecvAll(r, nombreLen)
	if err != nil {
		return "", 0, err
	}
	nombre := string(bufNombre)
	bytesRead += nombreLen

	bufLen, err = safe_socket.RecvAll(r, 1)
	if err != nil {
		return "", 0, err
	}
	apellidoLen := int(bufLen[0])
	bytesRead += 1

	bufApellido, err := safe_socket.RecvAll(r, apellidoLen)
	if err != nil {
		return "", 0, err
	}
	apellido := string(bufApellido)
	bytesRead += apellidoLen

	bufDoc, err := safe_socket.RecvAll(r, 4)
	if err != nil {
		return "", 0, err
	}
	documento := binary.BigEndian.Uint32(bufDoc)
	bytesRead += 4

	bufFecha, err := safe_socket.RecvAll(r, 4)
	if err != nil {
		return "", 0, err
	}
	anio := binary.BigEndian.Uint16(bufFecha[0:2])
	mes := bufFecha[2]
	dia := bufFecha[3]
	bytesRead += 4

	bufNum, err := safe_socket.RecvAll(r, 2)
	if err != nil {
		return "", 0, err
	}
	numero := binary.BigEndian.Uint16(bufNum)
	bytesRead += 2

	line := fmt.Sprintf("%s,%s,%d,%04d-%02d-%02d,%d\n", nombre, apellido, documento, anio, mes, dia, numero)
	return line, bytesRead, nil
}