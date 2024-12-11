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
        self.client_socket.settimeout(timeout_ms / 1000.0)
        try:
            return self.client_socket.recv(1024).decode("utf-8")
        except socket.timeout:
            return ""
        except socket.error as e:
            return ""

    def start(self):
        if not self.is_port_open():
            print(f"Port {self.port} is not open. Exiting.")
            return

        print(f"Connecting to {self.host} on port {self.port}...")
        self.client_socket.connect((self.host, self.port))

        try:
            while True:
                # terima dan print pesan dari server
                server_message = self.receive_message(timeout_ms=50)
                if server_message:
                    print(f"{server_message}", end="")

                # ngirim query ke server
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
