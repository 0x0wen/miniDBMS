import socket

class Client:
    def __init__(self, host="localhost", port=1235):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def is_port_open(self):
        """Check if the specified port is open."""
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.settimeout(1)
        result = test_socket.connect_ex((self.host, self.port))
        test_socket.close()
        return result == 0

    def accept_query(self):
        query_input = ""
        print("> ", end="")
        while True:
            data = input()
            if ';' in data:
                query_input += data[:data.index(';')]
                break
            query_input += (data + " ")
        return query_input.strip()

    def receive_message(self, timeout_ms=50):
        # print("masok")
        """Receive a message from the server, ensuring full message retrieval."""
        try:
            # Set timeout for the socket
            self.client_socket.settimeout(timeout_ms / 1000.0)

            # Read the message length (4 bytes, big-endian)
            header = self.client_socket.recv(4)
            if not header:
                return ""

            message_length = int.from_bytes(header, byteorder="big")

            # Read the full message based on its length
            received_data = bytearray()
            while len(received_data) < message_length:
                packet = self.client_socket.recv(message_length - len(received_data))
                if not packet:
                    break
                received_data.extend(packet)

            return received_data.decode("utf-8")
        except socket.timeout:
            # Return empty string on timeout
            return ""
        except socket.error:
            # Return empty string on other socket errors
            return ""

    def start(self):
        # if not self.is_port_open():
        #     print(f"Port {self.port} is not open. Exiting.")
        #     return

        print(f"Connecting to {self.host} on port {self.port}...")
        self.client_socket.connect((self.host, self.port))
        first = True

        try:
            while True:
                # Receive and print messages from server
                if first:
                    server_message = self.receive_message()
                    first = False
                else:
                    server_message = self.receive_message(10000)
                if server_message:
                    print(f"{server_message}", end="")

                # Send query to server
                user_input = self.accept_query()
                while not user_input:
                    user_input = self.accept_query()

                self.client_socket.send(user_input.encode("utf-8"))
        except KeyboardInterrupt:
            print("\nDisconnecting...")
        finally:
            self.client_socket.close()

if __name__ == "__main__":
    client = Client()
    client.start()
