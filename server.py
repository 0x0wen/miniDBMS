import socket
import threading
from QueryProcessor.QueryProcessor import QueryProcessor

class Server:
    def __init__(self, host="localhost", port=12345):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.query_processor = QueryProcessor()

    def handle_client(self, client_socket):
        try:
            while True:
                # terima input klien
                client_socket.send("> ".encode("utf-8"))
                query_input = client_socket.recv(1024).decode("utf-8").strip()
                if not query_input:
                    continue

                # query diproses dan dioptimize
                queries = [query_input]
                if query_input.upper().startswith("BEGIN TRANSACTION"):
                    while True:
                        client_socket.send("> ".encode("utf-8"))
                        query_input = client_socket.recv(1024).decode("utf-8").strip()
                        queries.append(query_input)
                        if query_input.upper() == "COMMIT":
                            break

                optimized_query = self.query_processor.execute_query(queries)

                transaction_id = self.query_processor.concurrent_manager.beginTransaction()
                print(f"Transaction ID: {transaction_id}")
                rows = self.query_processor.generate_rows_from_query_tree(optimized_query, transaction_id)
                print(rows.data)
                self.query_processor.concurrent_manager.logObject(rows, transaction_id)
                print("Transaction has been logged.")

                # kriim hasil ke klien
                for q in optimized_query:
                    client_socket.send(f"Optimized Query Tree: {q.query_tree}\n".encode("utf-8"))
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server running on {self.host}:{self.port}")
        while True:
            client_socket, client_address = self.server_socket.accept()
            print(f"Client connected: {client_address}")
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.start()

if __name__ == "__main__":
    server = Server()
    server.start()
