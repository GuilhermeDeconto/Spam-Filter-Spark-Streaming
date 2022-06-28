import time
import json
import socket
import sys
from itertools import islice


TCP_IP = "localhost"
TCP_PORT = 6100


def connectTCP():   
    # connect to the TCP server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print(f"Waiting for connection on port {TCP_PORT}...")
    connection, address = s.accept()
    print(f"Connected to {address}")
    return connection, address

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def streamJsonFile(tcp_connection, input_file, batchSize):
    with open(input_file, 'rb') as f:
        for n_lines in iter(lambda: tuple(islice(f, batchSize)), ()):
            list = []
            for item in n_lines:
                data = json.loads(item)
                list.append(data)
            value = (json.dumps(list) + '\n').encode()
            try:
                tcp_connection.send(value) 
            except BrokenPipeError:  # this indicates that the message length of the payload is more than what is allowed via TCP
                print("Either batch size is too big for the dataset or the connection was closed")
            except Exception as error_message:
                print(f"Exception thrown but was handled: {error_message}")
            time.sleep(1)


if __name__ == '__main__':
    input_file = "/home/ubuntu/Spam-Filter-Spark-Streaming/datasets/output.json"

    tcp_connection, _ = connectTCP()

    batchSize = 3

    if len(sys.argv[1]) >= 1:
        batchSize = int(sys.argv[1])

    if (len(sys.argv[2]) >= 1):
        for times in range(int(sys.argv[2])):
            streamJsonFile(tcp_connection, input_file, batchSize)
    else:
        streamJsonFile(tcp_connection, input_file, batchSize)

    tcp_connection.close()