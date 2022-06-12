import socket
import json

class Message:
    def __init__(self, type, value):
        self.type = type
        self.value = value

DataType , IP1 , PORT1 , IP2 , PORT2 = input().split(" ")
PORT1 = int(PORT1)
PORT2 = int(PORT2)
Socket = socket.socket()
Socket.bind((IP1, PORT1))
# Sending Socket
if DataType == "s":
    SenderMessage = Message("start", "hello")   # Create a Message
    Socket.connect((IP2, PORT2))                # Connect to Recieving Socket 
    byte_array = json.dumps(SenderMessage.__dict__).encode("utf-8")
    Socket.send(byte_array)                     # Sending 'hello'
    SenderMsg = Socket.recv(1024)               # Recieving 'hi'
    ReceivedMessage = Message(**json.loads(SenderMsg))
    print(ReceivedMessage.value, ReceivedMessage.type)
    SenderMessage = Message("end", "goodbye")   # Sending 'goodbye' to end connection
    byte_array = json.dumps(SenderMessage.__dict__).encode("utf-8")
    Socket.send(byte_array)
    SenderMsg = Socket.recv(1024)
    ReceivedMessage = Message(**json.loads(SenderMsg)) # Recieving 'bye'
    print(ReceivedMessage.value, ReceivedMessage.type)
    if ReceivedMessage.type == "end":           # ending connection
        Socket.close()
        
# Recieving Socket        
if DataType == "r":
    Socket.listen(5)
    connection, address = Socket.accept()       # wait until one socket make a connection
    if address == (IP2, PORT2):                 # check if the socket is one we are waiting for
        while True:
            ReceiverMsg = connection.recv(1024) # Recieving 'hello'
            if not ReceiverMsg:
                break
            ReceivedMessage = Message(**json.loads(ReceiverMsg))
            print(ReceivedMessage.value, ReceivedMessage.type)
            if ReceivedMessage.type == "start":     
                ReceiverMessage = Message("start", "hi")  # Create a Message 
                byte_array = json.dumps(ReceiverMessage.__dict__).encode("utf-8")
                connection.send(byte_array)               # Sending 'hello'
            if ReceivedMessage.type == "end":           
                ReceiverMessage = Message("end", "bye")   # Sending 'goodbye'
                byte_array = json.dumps(ReceiverMessage.__dict__).encode("utf-8")
                connection.send(byte_array)