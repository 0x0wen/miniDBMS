import unittest
import socket
import threading


class ConcurrentClient:
    def __init__(self, host="localhost", port=1235, input_user=None, id_user=None):
        if input_user is None:
            self.input_user = [""]
        else:
            self.input_user = input_user
        if id_user is None:
            self.id_user = -1
        else:
            self.id_user = id_user
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

    def accept_query(self, input_index):
        query_input = self.input_user[input_index]
        return query_input.strip()

    def receive_message(self, timeout_ms=50):
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
        print(f"Connecting to {self.host} on port {self.port}...")
        self.client_socket.connect((self.host, self.port))
        first = True

        try:
            for i in range(len(self.input_user)):
                # Receive and print messages from server
                if first:
                    server_message = self.receive_message()
                    first = False
                else:
                    server_message = self.receive_message(10000)
                if server_message:
                    print(f"Received by {self.id_user} in Input \n {server_message}", end="")

                # Send query to server
                user_input = self.accept_query(i)
                while not user_input:
                    user_input = self.accept_query(i)

                print(f"input sent by {self.id_user}: {user_input}")
                self.client_socket.send(user_input.encode("utf-8"))

            for i in range(len(self.input_user)):
                server_message = self.receive_message(10000)
                if server_message:
                    print(f"Received by {self.id_user} in Output \n {server_message}", end="")

        except KeyboardInterrupt:
            print("\nDisconnecting...")
        finally:
            self.client_socket.close()


class ConcurrentTest(unittest.TestCase):
    def test_concurrent(self):
        client_1_input = [
            "BEGIN TRANSACTION",
            "SELECT * FROM student WHERE studentid = 1",
            "COMMIT"
        ]
        client_1 = ConcurrentClient(input_user=client_1_input, id_user=1)

        client_2_input = [
            "BEGIN TRANSACTION",
            "SELECT * FROM student WHERE studentid = 1",
            "UPDATE student SET fullname = 'JohnDoe' WHERE studentid = 1",
            "SELECT * FROM student WHERE studentid = 1",
            "COMMIT"
        ]
        client_2 = ConcurrentClient(input_user=client_2_input, id_user=2)

        client_3_input = [
            "BEGIN TRANSACTION",
            "SELECT * FROM student WHERE studentid = 1",
            "COMMIT"
        ]
        client_3 = ConcurrentClient(input_user=client_3_input, id_user=3)

        thread_1 = threading.Thread(target=client_1.start)
        thread_2 = threading.Thread(target=client_2.start)
        thread_3 = threading.Thread(target=client_3.start)

        thread_1.start()
        thread_2.start()
        thread_3.start()

        thread_1.join()
        thread_2.join()
        thread_3.join()




if __name__ == '__main__':
    unittest.main()
