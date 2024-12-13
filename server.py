from Interface.Rows import Rows
from Interface.Action import Action
from FailureRecovery.Structs.RecoverCriteria import RecoverCriteria
from QueryProcessor.QueryProcessor import QueryProcessor
import socket
import time
import threading
from collections import deque


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
        self.transactionid_to_clientid = {}
        self.clientid_to_transactionid = {}
        self.clientid_to_queries = {}  # dictionary to store optimized query and its transaction id
        self.clientid_to_clientsocket = {}
        self.last_transaction_id = 0
        self.transaction_map_of_deque = {}

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

    def generate_rows(self, query, client_id):  # dipanggil setelah semua query dimasukkan
        # BEGIN OPTIMIZING
        optimized_query = []
        statistics = self.query_processor.storage_manager.getStats()
        for q in query:
            query_without_aliases, alias_map = self.query_processor.remove_aliases(q)
            optimized_query.append(
                self.query_processor.optimization_engine.optimizeQuery(
                    self.query_processor.optimization_engine.parseQuery(q, statistics), statistics))
            # self.optimization_engine.optimizeQuery(self.optimization_engine.parseQuery(query_without_aliases)))
        # END OPTIMIZING
        print("optimized querynya adalah")
        for q in optimized_query:
            print(q.query_tree)

        # BEGIN CONCURRENCY CONTROL
        # Get transaction ID
        transaction_id = self.query_processor.concurrent_manager.beginTransaction()
        print(f"Transaction ID: {transaction_id}")
        self.transactionid_to_clientid[transaction_id] = client_id
        self.clientid_to_transactionid[client_id] = transaction_id

        # Generate Rows object from optimized query
        print("atas")
        rows = self.query_processor.generate_rows_from_query_tree(optimized_query, transaction_id)
        print("bawah")
        print(rows.data)
        return rows.data, optimized_query

    def run_all(self, queries, client_id, client_socket):
        # print("queries", queries)
        rows_data, optimized_query = self.generate_rows(queries, client_id)
        # print("isi dict trans id to client id", self.transactionid_to_clientid)
        # print("isi dict client id to trans id", self.clientid_to_transactionid)
        # print("optimized query:", optimized_query)
        # print("rows data:", rows_data)
        end = False
        for row_data, single_query in zip(rows_data, optimized_query):
            print(self.transaction_map_of_deque)
            # print("anu 1", single_query)
            # print("anu 2", queries[len(queries) - 1])
            if single_query.query_tree.node_type == "COMMIT" or len(queries) == 1:
                end = True
                print("masuk end")
            # else:
            # print("single query:", single_query)
            # print("single query:", single_query.query_tree.node_type)
            # Create a Rows object with a single row of data
            single_row = Rows([row_data])

            # Create an Action object based on the first character of the row string
            row_string = single_row.data[0]
            action_type = "read" if row_string[0] == 'R' else "write"
            action = Action([action_type])
            self.query_processor.concurrent_manager.logObject(single_row, self.clientid_to_transactionid[client_id])
            response = self.query_processor.concurrent_manager.validateObject(single_row,
                                                                              self.clientid_to_transactionid[client_id],
                                                                              action)
            print("response nya", response, " saat query:", single_query)
            # print("dari query", single_query)

            # Cek apakah transaksi ini sudah harus menunggu atau langsung dijalankan
            is_run = False
            for id_trans in self.transaction_map_of_deque:
                if id_trans == response.related_t_id:
                    print("transaction id ada di queue:", self.clientid_to_transactionid[client_id])
                    if response.response_action == "ALLOW":
                        self.transaction_map_of_deque[id_trans].append(single_query)
                        while self.transaction_map_of_deque[id_trans]:
                            single_query = self.transaction_map_of_deque[id_trans].popleft()
                            self.query_processor.concurrent_manager.logObject(single_row,
                                                                              self.clientid_to_transactionid[client_id])
                            response = self.query_processor.concurrent_manager.validateObject(single_row,
                                                                                              self.clientid_to_transactionid[
                                                                                                  client_id], action)
                            print("Transaksi ini responsenya", response)
                            start_time = time.time()
                            send_to_client = self.query_processor.execute_query(single_query, client_id,
                                                                                self.clientid_to_transactionid[
                                                                                    client_id])
                            execution_time = time.time() - start_time
                            send_to_client += (f"\nExecution Time: {execution_time:.3f} ms\n")
                            self.send_with_header(client_socket, send_to_client)

                        is_run = True
                    elif response.response_action == "WAIT":
                        # Kalau wait maka masukkan ke deque
                        self.transaction_map_of_deque[id_trans].append(single_query)
                        is_run = True
                    elif response.response_action == "WOUND":
                        # Kalau wound maka abort semua transaksi yang berkaitan
                        self.transaction_map_of_deque[id_trans].clear()
            if is_run:
                continue

            if response.response_action == "ALLOW":
                print("transaction id yg allowed:", self.clientid_to_transactionid[client_id],
                      "related id di response:", response.related_t_id, "current id di response:",
                      response.current_t_id)
                # print("ketika querynya:", single_query)
                # run directly
                start_time = time.time()
                send_to_client = self.query_processor.execute_query(single_query, client_id,
                                                                    self.clientid_to_transactionid[client_id])
                execution_time = time.time() - start_time
                send_to_client += (f"\nExecution Time: {execution_time:.3f} ms\n")
                self.send_with_header(client_socket, send_to_client)
                # self.query_processor.concurrent_manager.end(self.clientid_to_transactionid[client_id])
            elif response.response_action == "WAIT":
                # Kalau wait maka transaksi ini harus menunggu transaksi lain
                # Masukkan query transaksi ini ke dalam deque
                self.transaction_map_of_deque[response.current_t_id] = deque()
                self.transaction_map_of_deque[response.current_t_id].append(single_query)

                # Melanjutkan ke transaksi lain

                # print("transaction id yg wait:", self.clientid_to_transactionid[client_id], "related id di response:", response.related_t_id, "current id di response:", response.current_t_id)
                # print("dia wait transaction id", response.related_t_id, "yg dijalankan oleh client", self.transactionid_to_clientid[response.related_t_id])
                # print("ketika querynya:", single_query)
                # # letting other transaction run while continuously check for response change
                # while response.response_action != "ALLOW":
                #     print("waiting")
                #     self.query_processor.concurrent_manager.logObject(single_row, self.clientid_to_transactionid[client_id])
                #     response = self.query_processor.concurrent_manager.validateObject(single_row, self.clientid_to_transactionid[client_id], action)
                #     # print(response.response_action)
                # print("transaction", self.clientid_to_transactionid[client_id],"dah jalan")
                # start_time = time.time()
                # send_to_client = self.query_processor.execute_query(single_query, client_id, self.clientid_to_transactionid[client_id])
                # execution_time = time.time() - start_time
                # send_to_client += (f"\nExecution Time: {execution_time:.3f} ms\n")
                # self.send_with_header(client_socket, send_to_client)
                # self.query_processor.concurrent_manager.end(self.clientid_to_transactionid[client_id])
            elif response.response_action == "WOUND":
                print("transaction id yg wounded:", self.clientid_to_transactionid[client_id],
                      "related id di response:", response.related_t_id, "current id di response:",
                      response.current_t_id)
                # print("ketika querynya:", single_query)
                # abort all changes on related transaction id
                self.query_processor.failure_recovery.recover(RecoverCriteria(transaction_id=response.current_t_id))
                self.query_processor.concurrent_manager.end(response.current_t_id)
                # run current query
                start_time = time.time()
                send_to_client = self.query_processor.execute_query(single_query, client_id,
                                                                    self.clientid_to_transactionid[client_id])
                execution_time = time.time() - start_time
                send_to_client += (f"\nExecution Time: {execution_time:.3f} ms\n")
                self.send_with_header(client_socket, send_to_client)
                # self.query_processor.concurrent_manager.end(self.clientid_to_transactionid[client_id])
                # start over related transaction
                self.run_all(self.clientid_to_queries[self.transactionid_to_clientid[response.current_t_id]],
                             self.transactionid_to_clientid[response.related_t_id],
                             self.clientid_to_clientsocket[self.transactionid_to_clientid[response.related_t_id]])

        if end:
            print("query", self.clientid_to_transactionid[client_id], "dah commit dan end")
            self.query_processor.concurrent_manager.endTransaction(self.clientid_to_transactionid[client_id])

        # after transaction done run this
        if self.query_processor.failure_recovery.logManager.is_wal_full():
            self.query_processor.storage_manager.synchronize_storage()
            self.timer_event.set()  # restart timer 300 detik
        # remove client id and transaction id map
        self.clientid_to_transactionid = {}
        self.transactionid_to_clientid = {}

    def handle_client(self, client_socket, client_id):
        try:
            while True:
                query_input = client_socket.recv(1024).decode("utf-8").strip()

                if not query_input:
                    continue

                try:
                    # accept queries from user
                    queries = [query_input]
                    if query_input.upper().startswith("BEGIN TRANSACTION"):
                        while True:
                            self.send_with_header(client_socket, "enter COMMIT; if you're done\n")
                            query_input = client_socket.recv(1024).decode("utf-8").strip()
                            queries.append(query_input)
                            if query_input.upper() == "COMMIT":
                                queries.pop(0)
                                # queries.pop()
                                break

                    # main function to run queries
                    self.run_all(queries, client_id, client_socket)

                except Exception as e:
                    # handle query errors without disconnecting the client
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
