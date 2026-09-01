import socket
import logger
import safe_socket
import threading
import os
import signal
from lottery.lottery import Lottery
from lottery.bet import Bet

from protocol.protocol import (
    ClientProtocol,
    MSG_TYPE_BET,
    MSG_TYPE_END,
    MSG_TYPE_WINNERS,
    MSG_TYPE_ACK,
)


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
        protocol = ClientProtocol(client_socket)
        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.running:
                msg_type, payload = protocol.receive_message()
                if msg_type is None:
                    break

                if msg_type == MSG_TYPE_BET:
                    agency_id, bets_batch = protocol.parse_bets_payload(payload)
                    if agency_id is not None:
                        current_agency_id = agency_id

                    if bets_batch:
                        with self.storage_lock:
                            self.lottery.store_bets(bets_batch)
                        message_amount += len(bets_batch)

                    protocol.send_ack()

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
                            winners.append(bet)

                    protocol.send_winners(winners)

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
            protocol.close()

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