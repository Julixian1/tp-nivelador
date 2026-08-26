import socket
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet

MSG_TYPE_BET = 1
MSG_TYPE_END = 2
MSG_TYPE_WINNERS = 3


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path="bets.csv")

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        current_agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                header = safe_socket.recv_all(client_socket, 3)
                if not header:
                    break

                payload_len = int.from_bytes(header[0:2], byteorder="big")
                msg_type = header[2]

                payload = b""
                if payload_len > 0:
                    payload = safe_socket.recv_all(client_socket, payload_len)

                if msg_type == MSG_TYPE_BET:
                    line = payload.decode("utf-8")
                    parts = line.split(",")
                    current_agency_id = int(parts[0])

                    bet = Bet(
                        agency_id=current_agency_id,
                        first_name=parts[1],
                        last_name=parts[2],
                        document=int(parts[3]),
                        birthdate=parts[4],
                        number=int(parts[5]),
                    )

                    self.lottery.store_bets([bet])
                    message_amount += 1

                elif msg_type == MSG_TYPE_END:
                    winners = [
                        str(bet.document)
                        for bet in self.lottery.load_bets()
                        if bet.agency_id == current_agency_id and self.lottery.has_won(bet)
                    ]

                    winners_payload = "\n".join(winners).encode("utf-8")
                    if len(winners) > 0:
                        winners_payload += b"\n"

                    resp_len = len(winners_payload).to_bytes(2, byteorder="big")
                    resp_type = bytes([MSG_TYPE_WINNERS])
                    response = resp_len + resp_type + winners_payload

                    safe_socket.send_all(client_socket, response)

                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                        "winners-amount",
                        len(winners),
                    )
                    return
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e
        finally:
            client_socket.close()

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
