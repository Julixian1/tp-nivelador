import safe_socket
from lottery.bet import Bet

MSG_TYPE_BET = 1
MSG_TYPE_END = 2
MSG_TYPE_WINNERS = 3
MSG_TYPE_ACK = 4

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
            if payload is None:
                return None, None

        return msg_type, payload

    def send_ack(self):
        ack_packet = bytes([MSG_TYPE_ACK])
        return safe_socket.send_all(self.sock, ack_packet)

    def send_winners(self, winners_list):
        winners_payload = "\n".join(winners_list).encode("utf-8")
        if len(winners_list) > 0:
            winners_payload += b"\n"

        resp_len = len(winners_payload).to_bytes(2, byteorder="big")
        resp_type = bytes([MSG_TYPE_WINNERS])
        response = resp_len + resp_type + winners_payload

        return safe_socket.send_all(self.sock, response)

    def parse_bets_payload(self, payload):
        lines = payload.decode("utf-8").split("\n")
        bets_batch = []
        agency_id = None

        for line in lines:
            if not line.strip():
                continue
            parts = line.split(",")
            agency_id = int(parts[0])

            bet = Bet(
                agency_id=agency_id,
                first_name=parts[1],
                last_name=parts[2],
                document=int(parts[3]),
                birthdate=parts[4],
                number=int(parts[5]),
            )
            bets_batch.append(bet)

        return agency_id, bets_batch

    def close(self):
        self.sock.close()