import socket

class Client:
    def __init__(self, host="localhost", port=12346):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def accept_query(self):
        query_input = ""
        # print("> ", end="")
        while True:
            data = input()
            if ';' in data:
                query_input += data[:data.index(';')]
                break
            query_input += (data + " ")
        query_input = query_input.strip()

        return query_input

    def start(self):
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

                # ngirim ke server
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
