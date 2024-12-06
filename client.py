import socket

class Client:
    def __init__(self, host="localhost", port=1234):
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
        while True:
            data = input()
            if ';' in data:
                query_input += data[:data.index(';')]
                break
            query_input += (data + " ")
        query_input = query_input.strip()
        return query_input

    def start(self):
        if not self.is_port_open():
            print(f"Port {self.port} is not open. Exiting.")
            return

        print(f"Connecting to {self.host} on port {self.port}...")
        self.client_socket.connect((self.host, self.port))

        try:
            accept = False
            while True:
                if accept:
                    server_message = self.client_socket.recv(1024).decode("utf-8")
                    print(server_message, end="(dari atas)\n")
                    accept = False

                server_message = self.client_socket.recv(1024).decode("utf-8")
                print(server_message, end="(dari bawah)\n")

                # Sending to server
                user_input = self.accept_query()
                while not user_input:
                    print("> ", end="")
                    user_input = self.accept_query()
                if self.client_socket.send(user_input.encode("utf-8")):
                    accept = True
                    print("sending bro")
                else:
                    print("ga sending bro")
        except KeyboardInterrupt:
            print("\nDisconnecting...")
        finally:
            self.client_socket.close()

if __name__ == "__main__":
    client = Client()
    client.start()
