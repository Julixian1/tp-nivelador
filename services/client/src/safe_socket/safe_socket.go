package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	totalWritten := 0
	bytesToWrite := len(bytes)

	for totalWritten < bytesToWrite {
		n, err := socket.Write(bytes[totalWritten:])
		if err != nil {
			return err
		}
		totalWritten += n
	}
	
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	totalRead := 0
	buff := make([]byte, size)

	for totalRead < size {
		n, err := socket.Read(buff[totalRead:])
		if err != nil {
			return nil, err
		}
		totalRead += n
	}
	
	return buff, nil

}
