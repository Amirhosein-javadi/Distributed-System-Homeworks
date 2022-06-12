from threading import Thread
import socket
from queue import Queue
import json
import time


class Message:
    def __init__(self, Type, Value, Length):
        self.Type         = Type      # ELECTION Or LEADER Or RETURN
        self.Value        = Value     # Node's UID
        self.Length       = Length    # Throwing range


class MyThread(Thread):
    def __init__(self, Uid, Delay):
        super().__init__(name=f'Process {Uid} Thread')
        self.Uid          = Uid
        self.Delay        = Delay
        self.Receiving    = Receive(self)   # Receiving mechanism
        self.Sending      = Send(self)      # Sending mechanism
        self.SendingQueue = Queue()         # Storing mechanism using queueing
        self.Round        = 0               
        self.Length       = 2 ** self.Round        
        self.ReturnedUid  = 0               # Determined if two tokens has returned to Node
        self.Leader       = None

    def Sibilings(self,NextNode,PreviousNode):
        self.NextNode     = NextNode        # Next node
        self.PreviousNode = PreviousNode    # Previous Node

    def run(self):
        SenderMessage     = Message('ELECTION' ,self.Uid ,self.Length)  # Creating first massage with one's UID       
        self.SendingQueue.put((SenderMessage, self.NextNode.Receiving.Address)) # insert massage in queue of sending
        self.SendingQueue.put((SenderMessage, self.PreviousNode.Receiving.Address))
        self.Receiving.start()
        self.Sending.start()
        self.Receiving.join()
        self.Sending.join()


class Receive(Thread):
    def __init__(self, Process):
        self.Process     = Process    
        super().__init__(name=f'Receive {Process.Uid} Thread')
        self.Socket      = socket.socket()  
        self.Ip          = socket.gethostname()
        self.Port        = 0 
        self.Socket.bind((self.Ip,self.Port))  
        self.Address     = self.Socket.getsockname()

    def run(self):
        while self.Process.Leader == None:  # sleep if you know the leader
            self.Socket.listen(5)                                 
            connection, address   = self.Socket.accept()    # wait until one socket make a connection    
            ReceivedMessage       = Message(**json.loads(connection.recv(1024)))              
            if address == self.Process.NextNode.Sending.Address:  # had Next node sent the massage?
                print(f'node{self.Process.Uid} received from node{self.Process.NextNode.Uid} : '+ReceivedMessage.Type+' ' +str(ReceivedMessage.Value)) 
            elif address == self.Process.PreviousNode.Sending.Address: # had PreviousNode node sent the massage?
                print(f'node{self.Process.Uid} received from node{self.Process.PreviousNode.Uid} : '+ReceivedMessage.Type+' ' +str(ReceivedMessage.Value)) 
            
            if ReceivedMessage.Type == 'ELECTION':
                ReceivedMessage.Length = ReceivedMessage.Length-1     
                if ReceivedMessage.Value == self.Process.Uid:         # leader detected
                    self.Process.Leader = ReceivedMessage.Value
                    while not self.Process.SendingQueue.empty():      # clear the leader's queue
                        self.Process.SendingQueue.get()
                    SenderMessage       = Message('LEADER', ReceivedMessage.Value, ReceivedMessage.Length)  # send leader massage to neighbours
                    self.Process.SendingQueue.put((SenderMessage, self.Process.PreviousNode.Receiving.Address))
                    self.Process.SendingQueue.put((SenderMessage, self.Process.NextNode.Receiving.Address))
                    self.Socket.close()

                elif ReceivedMessage.Length > 0:
                    if self.Process.Uid < ReceivedMessage.Value:   # if received massage is greater than Uid, then just pass it
                        SenderMessage       = Message('ELECTION', ReceivedMessage.Value, ReceivedMessage.Length)  
                        if address == self.Process.NextNode.Sending.Address:
                            self.Process.SendingQueue.put((SenderMessage, self.Process.PreviousNode.Receiving.Address))
                        elif address == self.Process.PreviousNode.Sending.Address:
                            self.Process.SendingQueue.put((SenderMessage, self.Process.NextNode.Receiving.Address))
                else:
                    if self.Process.Uid < ReceivedMessage.Value:  # if massage's length is 0, then return the token
                        SenderMessage       = Message('RETURN', ReceivedMessage.Value,1)
                        if address == self.Process.NextNode.Sending.Address:
                            self.Process.SendingQueue.put((SenderMessage, self.Process.NextNode.Receiving.Address))
                        elif address == self.Process.PreviousNode.Sending.Address:
                            self.Process.SendingQueue.put((SenderMessage, self.Process.PreviousNode.Receiving.Address))

            elif ReceivedMessage.Type == 'RETURN':
                if ReceivedMessage.Value == self.Process.Uid:
                    self.Process.ReturnedUid       = self.Process.ReturnedUid+1
                    if self.Process.ReturnedUid   == 2:
                        self.Process.ReturnedUid   = 0
                        self.Process.Round         = self.Process.Round+1
                        MessageLength              = 2 ** self.Process.Round
                        SenderMessage              = Message('ELECTION', self.Process.Uid, MessageLength)
                        self.Process.SendingQueue.put((SenderMessage, self.Process.NextNode.Receiving.Address))
                        self.Process.SendingQueue.put((SenderMessage, self.Process.PreviousNode.Receiving.Address))
                else:
                    SenderMessage = Message('RETURN', ReceivedMessage.Value, ReceivedMessage.Length)
                    if address == self.Process.NextNode.Sending.Address:
                        self.Process.SendingQueue.put((SenderMessage, self.Process.PreviousNode.Receiving.Address))
                    elif address == self.Process.PreviousNode.Sending.Address:
                        self.Process.SendingQueue.put((SenderMessage, self.Process.NextNode.Receiving.Address))
            else:
                while not self.Process.SendingQueue.empty():
                    self.Process.SendingQueue.get()
                SenderMessage = Message('LEADER', ReceivedMessage.Value, ReceivedMessage.Length) 
                if address == self.Process.NextNode.Sending.Address:
                    self.Process.SendingQueue.put((SenderMessage, self.Process.PreviousNode.Receiving.Address))
                elif address == self.Process.PreviousNode.Sending.Address:
                    self.Process.SendingQueue.put((SenderMessage, self.Process.NextNode.Receiving.Address))
                self.Process.Leader = ReceivedMessage.Value
                self.Socket.close()
            connection.close()


class Send(Thread):
    def __init__(self, Process):
        self.Process    = Process
        super().__init__(name=f'Send {Process.Uid} Thread')
        self.Socket     = None
        self.Address    = None
        self.GoToSleep  = False

    def run(self):
        while(self.GoToSleep)  == False:
            if self.Process.Leader != None:   # if you know the leader  Send the last message and go to sleep
                self.GoToSleep  = True
                while not self.Process.SendingQueue.empty():
                    self.Socket     = socket.socket()
                    self.Ip         = socket.gethostname()
                    self.Port       = 0 
                    self.Socket.bind((self.Ip,self.Port))
                    self.Address    = self.Socket.getsockname()
                    message, addr   = self.Process.SendingQueue.get()
                    time.sleep(self.Process.Delay)      # sleep for node's Delay
                    try:
                        self.Socket.connect(addr)
                        self.Socket.send(json.dumps(message.__dict__).encode("utf-8"))
                    except:         # it's because the reciever node might be sleep
                        pass
                    finally:
                        self.Socket.close()

            elif not self.Process.SendingQueue.empty(): # if you don't know the leader  Send every massage in the Queue
                self.Socket     = socket.socket()
                self.Ip         = socket.gethostname()
                self.Port       = 0 
                self.Socket.bind((self.Ip,self.Port))
                self.Address    = self.Socket.getsockname()
                message, addr   = self.Process.SendingQueue.get()
                time.sleep(self.Process.Delay)
                try:
                    self.Socket.connect(addr)
                    self.Socket.send(json.dumps(message.__dict__).encode("utf-8"))
                except :  # it's because the reciever node might be sleep
                    pass
                finally:
                    self.Socket.close()


counter  = input('Enter the number of nodes: ')
counter = int(counter)
    
for i in range(1,counter+1):    
    globals()["Uid" + str(i)] , globals()["Delay" + str(i)] = input(f'Enter Uid,Delay for {i}th node: ').split(" ") 
    globals()["Uid" + str(i)]       = int(globals()["Uid" + str(i)])
    globals()["Delay" + str(i)]     = int(globals()["Delay" + str(i)])
# defining n Thread
    globals()["t"+str(i)]           = MyThread(globals()["Uid"+str(i)], globals()["Delay"+str(i)])

# Set Sibilings
globals()["t" + str(1)].Sibilings(globals()["t" + str(2)],globals()["t" + str(counter)],)
globals()["t" + str(counter)].Sibilings(globals()["t" + str(1)],globals()["t" + str(counter-1)])
for i in range(2,counter):
     globals()["t" + str(i)].Sibilings(globals()["t" + str(i+1)],globals()["t" + str(i-1)])

for i in range(1,counter+1):    
    globals()["t" + str(i)].start()
    time.sleep(0.1)
for i in range(1,counter+1):
    globals()["t" + str(i)].join()    
    time.sleep(0.1)

print(f'ELECTION is over. LEADER UID is {globals()["t" + str(1)].Leader}')  