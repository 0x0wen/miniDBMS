import socket
import threading
from QueryProcessor.QueryProcessor import QueryProcessor

class Server:
    def __init__(self, host="localhost", port=1234):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.query_processor = QueryProcessor()
        self.is_running = False

    def handle_client(self, client_socket):
        try:
            while True:
                client_socket.send("> ".encode("utf-8"))
                query_input = client_socket.recv(1024).decode("utf-8").strip()

                if not query_input:
                    continue

                try:
                    # optimasi query
                    queries = [query_input]
                    if query_input.upper().startswith("BEGIN TRANSACTION"):
                        while True:
                            client_socket.send("enter COMMIT; if you're done\n".encode("utf-8"))
                            client_socket.send("> dariserver".encode("utf-8"))
                            query_input = client_socket.recv(1024).decode("utf-8").strip()
                            queries.append(query_input)
                            if query_input.upper() == "COMMIT":
                                break

                    optimized_query = self.query_processor.execute_query(queries)

                    # kirim hasil ke client
                    send_to_client = ""
                    for q in optimized_query:
                        send_to_client += (f"Optimized Query Tree: {q.query_tree}\n")
                    client_socket.send(send_to_client.encode("utf-8"))

                except Exception as e:
                    # biar ga diskonek kalo query error
                    error_message = f"Error processing query: {e}\n"
                    client_socket.send(error_message.encode("utf-8"))
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.is_running = True
        print(f"Server running on {self.host}:{self.port}")
        try:
            while self.is_running:
                client_socket, client_address = self.server_socket.accept()
                print(f"Client connected: {client_address}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_thread.start()
        except KeyboardInterrupt:
            print("\nKeyboardInterrupt received. Shutting down the server...")
            self.stop()
        except Exception as e:
            print(f"Server error: {e}")
            self.stop()

    def stop(self):
        self.is_running = False
        self.server_socket.close()
        print("Server has been stopped.")

if __name__ == "__main__":
    server = Server()
    server.start()
