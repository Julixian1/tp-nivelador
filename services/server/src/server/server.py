import socket
import logger
import safe_socket
import threading
import os
import signal
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
        self.quorum_min = int(os.getenv("AGENCY_QUORUM_MIN", "6"))
        self.barrier = threading.Barrier(self.quorum_min)
        self.storage_lock = threading.Lock()

        self.running = True
        self.server_socket = None
        self.client_threads = []
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        signal.signal(signal.SIGINT, self._sigterm_handler)

    def _sigterm_handler(self, signum, frame):
        action = "graceful-shutdown"
        logger.info(action, logger.LogResult.in_progress, "signal", signum)
        self.running = False

        try:
            self.barrier.abort()
        except Exception:
            pass

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        current_agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.running:
                header = safe_socket.recv_all(client_socket, 3)
                if not header:
                    break

                payload_len = int.from_bytes(header[0:2], byteorder="big")
                msg_type = header[2]

                payload = b""
                if payload_len > 0:
                    payload = safe_socket.recv_all(client_socket, payload_len)
                    if payload is None:
                        break

                if msg_type == MSG_TYPE_BET:
                    lines = payload.decode("utf-8").split("\n")
                    bets_batch = []
                    for line in lines:
                        if not line.strip():
                            continue
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
                        bets_batch.append(bet)

                    if bets_batch:
                        with self.storage_lock:
                            self.lottery.store_bets(bets_batch)
                        message_amount += len(bets_batch)

                elif msg_type == MSG_TYPE_END:
                    logger.info("waiting-quorum", logger.LogResult.in_progress)
                    try:
                        self.barrier.wait()
                    except threading.BrokenBarrierError:
                        logger.warn("quorum-wait", logger.LogResult.fail, "reason", "barrier_aborted")
                        return
                    logger.info("quorum-reached", logger.LogResult.success)

                    winners = []
                    for bet in self.lottery.load_bets():
                        if bet.agency_id == current_agency_id and self.lottery.has_won(bet):
                            winner_line = f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
                            winners.append(winner_line)

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
            if self.running:
                logger.error(
                    action, logger.LogResult.fail, "messages-amount", message_amount
                )
                raise e
        finally:
            client_socket.close()

    def run(self):
        action = "accept-connection"
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_host, self.server_port))
        self.server_socket.listen()

        while self.running:
            try:
                logger.info(action, logger.LogResult.in_progress)
                client_socket, _ = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client, args=(client_socket,)
                )
                client_thread.start()
                self.client_threads.append(client_thread)
            except OSError:
                break
            except Exception as e:
                if self.running:
                    logger.error(action, logger.LogResult.fail)
                    raise e

        for thread in self.client_threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
                
        logger.info("graceful-shutdown", logger.LogResult.success)
