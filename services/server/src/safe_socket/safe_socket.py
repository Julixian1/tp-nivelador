import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size: int):
    buffer = bytearray()
    while len(buffer) < size:
        packet = socket.recv(size - len(buffer))
        if not packet:
            if len(buffer) == 0:
                return None
            raise RuntimeError("Error: La conexión se cortó a la mitad del mensaje")
        buffer.extend(packet)
    return bytes(buffer)


def send_all(socket: socket.socket, data: bytes):
    total_sent = 0
    while total_sent < len(data):
        sent = socket.send(data[total_sent:])
        total_sent += sent