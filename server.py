import socket
import time
import threading
from QueryProcessor.QueryProcessor import QueryProcessor

class Server:
    def __init__(self, host="localhost", port=1235):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.query_processor = QueryProcessor()
        self.is_running = False
        self.timer_event = threading.Event()
        self.client_id_counter = 1
        self.clients = {}  # Store client ID and (address, socket)

    def send_with_header(self, client_socket, message):
        """Send a message with a header indicating its length."""
        message_bytes = message.encode("utf-8")
        message_length = len(message_bytes)
        header = message_length.to_bytes(4, byteorder="big")
        client_socket.sendall(header + message_bytes)

    def send_to_client_by_id(self, client_id, message):
        """Send a message to a specific client by ID."""
        if client_id in self.clients:
            _, client_socket = self.clients[client_id]
            try:
                self.send_with_header(client_socket, message)
            except Exception as e:
                print(f"Error sending to client {client_id}: {e}")
        else:
            print(f"Client ID {client_id} not found.")

    def handle_client(self, client_socket, client_id):
        try:
            while True:
                query_input = client_socket.recv(1024).decode("utf-8").strip()

                if not query_input:
                    continue

                try:
                    # Process queries
                    queries = [query_input]
                    if query_input.upper().startswith("BEGIN TRANSACTION"):
                        while True:
                            self.send_with_header(client_socket, "enter COMMIT; if you're done\n")
                            query_input = client_socket.recv(1024).decode("utf-8").strip()
                            queries.append(query_input)
                            if query_input.upper() == "COMMIT":
                                break

                    # Process query results
                    start_time = time.time()
                    send_to_client, execution_results = self.query_processor.execute_query(queries)
                    execution_time = time.time() - start_time
                    send_to_client += (f"\nExecution Time: {execution_time:.3f} ms\n")
                    self.send_with_header(client_socket, send_to_client)

                    if self.query_processor.failure_recovery.logManager.is_wal_full():
                        self.query_processor.storage_manager.synchronize_storage()
                        self.timer_event.set()  # restart timer 300 detik

                except Exception as e:
                    # Handle query errors without disconnecting the client
                    error_message = f"Error processing query: {e}\n"
                    self.send_with_header(client_socket, error_message)
        except Exception as e:
            print(f"Error handling client {client_id}: {e}")
        finally:
            print(f"Client {client_id} disconnected.")
            client_socket.close()
            del self.clients[client_id]

    def start_timer(self):
        def timer_task():
            while self.is_running:
                self.timer_event.clear()
                is_set = self.timer_event.wait(timeout=300)
                if not is_set:  # If 5 minutes pass, call sync storage
                    self.query_processor.storage_manager.synchronize_storage()

        timer_thread = threading.Thread(target=timer_task, daemon=True)
        timer_thread.start()

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.is_running = True
        print(f"Server running on {self.host}:{self.port}")

        self.start_timer()

        try:
            while self.is_running:
                client_socket, client_address = self.server_socket.accept()
                client_id = self.client_id_counter
                self.clients[client_id] = (client_address, client_socket)
                self.client_id_counter += 1

                print(f"Client {client_id} connected: {client_address}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, client_id))
                client_thread.start()
        except KeyboardInterrupt:
            print("\nKeyboardInterrupt received. Shutting down the server...")
            self.stop()
        except Exception as e:
            print(f"Server error: {e}")
            self.stop()

    def stop(self):
        self.is_running = False
        self.timer_event.set()
        self.server_socket.close()
        print("Server has been stopped.")

if __name__ == "__main__":
    server = Server()
    server.start()
