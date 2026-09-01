import safe_socket
from lottery.bet import Bet

MSG_TYPE_BET = 1
MSG_TYPE_END = 2
MSG_TYPE_WINNERS = 3
MSG_TYPE_ACK = 4

CHUNK_SIZE = 512

class ClientProtocol:
    def __init__(self, sock):
        self.sock = sock

    def receive_message(self):
        header = safe_socket.recv_all(self.sock, 3)
        if not header:
            return None, None

        payload_len = int.from_bytes(header[0:2], byteorder="big")
        msg_type = header[2]

        payload = b""
        if payload_len > 0:
            payload = safe_socket.recv_all(self.sock, payload_len)
            if payload is None or len(payload) < payload_len:
                return None, None

        return msg_type, payload

    def send_ack(self):
        ack_packet = bytes([MSG_TYPE_ACK])
        return safe_socket.send_all(self.sock, ack_packet)

    def _serialize_winner_tlv(self, bet: Bet) -> bytearray:
        buf = bytearray()

        first_name_bytes = bet.first_name.encode("utf-8")
        buf.append(len(first_name_bytes))
        buf.extend(first_name_bytes)

        last_name_bytes = bet.last_name.encode("utf-8")
        buf.append(len(last_name_bytes))
        buf.extend(last_name_bytes)

        buf.extend(int(bet.document).to_bytes(4, byteorder="big"))

        parts = bet.birthdate.split("-")
        buf.extend(int(parts[0]).to_bytes(2, byteorder="big"))
        buf.append(int(parts[1]))
        buf.append(int(parts[2]))

        buf.extend(int(bet.number).to_bytes(2, byteorder="big"))
        return buf

    def send_winners(self, winners_list):
        chunk = bytearray()

        for bet in winners_list:
            bet_bytes = self._serialize_winner_tlv(bet)

            if len(chunk) + len(bet_bytes) > CHUNK_SIZE:
                resp_len = len(chunk).to_bytes(2, byteorder="big")
                resp_type = bytes([MSG_TYPE_WINNERS])
                safe_socket.send_all(self.sock, resp_len + resp_type + bytes(chunk))
                chunk = bytearray()

            chunk.extend(bet_bytes)

        if len(chunk) > 0:
            resp_len = len(chunk).to_bytes(2, byteorder="big")
            resp_type = bytes([MSG_TYPE_WINNERS])
            safe_socket.send_all(self.sock, resp_len + resp_type + bytes(chunk))

        end_len = (0).to_bytes(2, byteorder="big")
        resp_type = bytes([MSG_TYPE_WINNERS])
        safe_socket.send_all(self.sock, end_len + resp_type)

    def parse_bets_payload(self, payload):
        bets_batch = []
        agency_id = None
        offset = 0
        total_len = len(payload)

        while offset < total_len:
            agency_id = int.from_bytes(payload[offset:offset + 2], byteorder="big")
            offset += 2

            nombre_len = payload[offset]
            offset += 1
            first_name = payload[offset:offset + nombre_len].decode("utf-8")
            offset += nombre_len

            apellido_len = payload[offset]
            offset += 1
            last_name = payload[offset:offset + apellido_len].decode("utf-8")
            offset += apellido_len

            document = int.from_bytes(payload[offset:offset + 4], byteorder="big")
            offset += 4

            anio = int.from_bytes(payload[offset:offset + 2], byteorder="big")
            offset += 2
            mes = payload[offset]
            offset += 1
            dia = payload[offset]
            offset += 1

            number = int.from_bytes(payload[offset:offset + 2], byteorder="big")
            offset += 2

            birthdate = f"{anio:04d}-{mes:02d}-{dia:02d}"

            bet = Bet(
                agency_id=agency_id,
                first_name=first_name,
                last_name=last_name,
                document=document,
                birthdate=birthdate,
                number=number,
            )
            bets_batch.append(bet)

        return agency_id, bets_batch

    def close(self):
        self.sock.close()