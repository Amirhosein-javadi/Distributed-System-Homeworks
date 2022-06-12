from threading import Thread
import socket
socket.setdefaulttimeout(1)
from queue import Queue
import json
import time
import numpy as np
import random

class Message:
    def __init__(self, Type, Value):
        self.Type         = Type      
        self.Value        = Value  
        
        
class MyThread(Thread):
    def __init__(self,Edge,N,T_round):
        super().__init__(name=f'Process {Edge} Thread')
        self.UID=Edge               # node's uid
        self.Neighbour = {}         # node's neighbour
        self.NeighbourDelay = {}    # Delay of vertices
        self.Awake = True           # determine when node's state is determined
        self.Value = None           # random value
        self.Round = 1              # 1 , 2, 3
        self.Status = 'Unknown'     # Unknown, Winner, Looser
        self.N     = N              # number of nodes
        self.RoundTime = T_round    # time of each round
        self.Receiving    = Receive(self)   # Receiving mechanism
        self.Sending      = Send(self)      # Sending mechanism
        self.SendingQueue = Queue()
        self.Timer  = Timer(self)          # node's clock
    def Sibilings(self,Thread,Delay,Edge):
        self.Neighbour[Edge] = Thread
        self.NeighbourDelay[Edge] = Delay
                
    def run(self):
        self.Value = random.randrange(self.N**4)       # make a random value
        SenderMessage = Message('Sending',self.Value)
        for items in self.Neighbour.keys():
            self.SendingQueue.put((SenderMessage,self.Neighbour[items].Receiving.Address,self.NeighbourDelay[items]))   # send random value to neighbours
        self.Timer.start()        
        self.Receiving.start()
        self.Sending.start()
        self.Timer.join()
        self.Receiving.join()
        self.Sending.join() 

        
class Timer(Thread):
    def __init__(self,Process):  
        self.Process = Process 
        super().__init__(name=f'Timer {Process.UID} Thread')
    def run(self):
        start = time.time()
        while(self.Process.Awake)  == True:
            end = time.time()
            if (end-start) > self.Process.RoundTime:       # when we reach Round time restart timer
                self.Process.Round = self.Process.Round + 1 
                self.Process.Round = self.Process.Round - 3 *  (self.Process.Round>3)
                start = time.time()
        
            
class Receive(Thread):
    def __init__(self,Process):
        self.Process = Process    
        super().__init__(name=f'Receive {Process.UID} Thread')    
        self.Socket      = socket.socket()  
        self.Ip          = socket.gethostname()
        self.Port        = 0 
        self.Socket.bind((self.Ip,self.Port))  
        self.Address     = self.Socket.getsockname()    
    def run(self):
        self.message = {}
        while(self.Process.Awake)  == True:
            flag = True
            while self.Process.Round==1:
                if flag:
                    for i in range(len(self.Process.Neighbour)):
                        self.Socket.listen(5) 
                        try:
                            connection,address   = self.Socket.accept()    # wait until one socket make a connection              
                            ReceivedMessage       = Message(**json.loads(connection.recv(1024))) 
                            for items in self.Process.Neighbour.keys():
                                if address == self.Process.Neighbour[items].Sending.Address:   # determine which node has sent the message
                                    print(f'node{self.Process.UID} received from node{self.Process.Neighbour[items].UID} : number '+str(ReceivedMessage.Value)) 
                                    self.message[items] = ReceivedMessage.Value  
                                    
                        except:
                            pass
                    flag = False
                    self.Process.Status = 'Winner'    
                    for items in self.message:   #if there is a value in received message that is greater than  node's value
                        if self.message[items] >= self.Process.Value:
                            self.Process.Status = 'Unknown'
                            break               
            flag = True
            while self.Process.Round==2:
                while not self.Process.SendingQueue.empty():
                    self.Process.SendingQueue.get()
                if self.Process.Status == 'Winner':   
                    SenderMessage = Message('Winner',self.Process.Value)
                    for items in self.Process.Neighbour.keys():
                        self.Process.SendingQueue.put((SenderMessage, self.Process.Neighbour[items].Receiving.Address,self.Process.NeighbourDelay[items]))                        
                self.message = {} 
                while self.Process.Round==2: 
                    self.Socket.listen(5)
                    try:
                        connection, address   = self.Socket.accept() 
                        ReceivedMessage  = Message(**json.loads(connection.recv(1024)))   
                        for items in self.Process.Neighbour.keys(): 
                            if address == self.Process.Neighbour[items].Sending.Address:
                                self.message[items] = ReceivedMessage.Type
                                print(f'node{self.Process.UID} received from node{self.Process.Neighbour[items].UID} : Winner') 
                        if flag == True:
                            for items in self.message:
                                if self.message[items] == 'Winner':    
                                    self.Process.Status = 'Loser'
                                    
                                if self.Process.Status == 'Loser' and flag == True:
                                    flag = False
                                    SenderMessage = Message('Loser',self.Value)
                                    for items in self.Neighbour.keys():
                                        self.Process.SendingQueue.put((SenderMessage,self.Neighbour[items].Receiving.Address,self.NeighbourDelay[items]))             
                                    break
                    except:
                        pass 
                       
                      
            flag = True
            self.message = {}
            while self.Process.Round==3:
                if self.Process.Status == 'Unknown':
                    while self.Process.Round==3:       # if node's state is Loser
                        self.Socket.listen(5)
                        try:
                            connection, address   = self.Socket.accept()    
                            ReceivedMessage  = Message(**json.loads(connection.recv(1024)))
                            for items in self.Process.Neighbour.keys():              
                                if address == self.Process.Neighbour[items].Sending.Address:
                                    self.message[items] = ReceivedMessage.Type 
                                    print(f'node{self.Process.UID} received from node{self.Process.Neighbour[items].UID} : Loser') 
                                                
                                      
                            for key in self.message:
                                if  self.message[key] == 'Loser':  
                                    self.Process.Neighbour.pop(key)         # if tou get any loser message delete the vertice 
                                    self.Process.NeighbourDelay.pop(key)
                            print(len(self.Process.Neighbour))     
                            if len(self.Process.Neighbour)==0:              # if there are no vertice the node is a winner
                                print(f'node{self.Process.UID} : Winner')                         
                                self.Process.Awake = False
                                connection.close()
                                break
                            
                            elif flag == True:
                                flag = False
                                self.Process.Value = random.randrange(self.Process.N**4)   # if node's state is unknown send a new random variable to it's neighbour
                                SenderMessage = Message('Sending',self.Value)
                                for items in self.Process.Neighbour.keys():
                                    self.SendingQueue.put((SenderMessage, self.Neighbour[items].Receiving.Address,self.NeighbourDelay[items]))                       
                        except socket.timeout:
                            pass       
                        
            if self.Process.Status == 'Winner': #and flag == True
                while not self.Process.SendingQueue.empty():
                    self.Process.SendingQueue.get()                    
                print(f'node{self.Process.UID} : Winner') 
                self.Process.Awake = False          # if node's state is Loser go to sleep
                connection.close()
                break   
            elif self.Process.Status == 'Loser': #and flag == True
                while not self.Process.SendingQueue.empty():
                    self.Process.SendingQueue.get()                    
                print(f'node{self.Process.UID} : Loser') 
                self.Process.Awake = False          # if node's state is Loser go to sleep
                connection.close()
                break                         

class Send(Thread):
    def __init__(self, Process):
        self.Process    = Process
        super().__init__(name=f'Send {Process.UID} Thread')
        self.Socket     = None
        self.Address    = None
        self.maxdelay   = 0
        
    def run(self):
        while(self.Process.Awake)  == True:
            self.maxdelay = 0
            while not self.Process.SendingQueue.empty():
                self.Socket     = socket.socket()
                self.Ip         = socket.gethostname()
                self.Port       = 0 
                self.Socket.bind((self.Ip,self.Port))
                self.Address    = self.Socket.getsockname()
                message,addr,delay   = self.Process.SendingQueue.get() 
                if self.maxdelay<delay:
                    self.maxdelay = delay
                try:
                    self.Socket.connect(addr)
                    self.Socket.send(json.dumps(message.__dict__).encode("utf-8"))
                except :  # it's because the reciever node might be sleep
                    pass
                finally:
                    self.Socket.close()    
            time.sleep(self.maxdelay)                
                        
Edges = []
T_round = input('Enter the Round Time: ')
print('Enter Edges and Delays , Click ENTER key to quit:',end='')
counter = 1
while True:
    globals()["E" + str(counter)] = input().split(" ")
    if globals()["E" + str(counter)] == [''] :
        break
    Edges.append(globals()["E" + str(counter)][0])
    Edges.append(globals()["E" + str(counter)][1])   
    counter += 1
    
Edges = list(dict.fromkeys(Edges))
for items in Edges:
    globals()["t"+items] = MyThread(items,len(Edges),np.uint16(T_round))
    
for i in range(1,counter): 
    globals()["t"+globals()["E"+str(i)][0]].Sibilings(globals()["t"+globals()["E"+str(i)][1]],np.uint16(globals()["E"+str(i)][2]),np.uint16(globals()["E"+str(i)][1]))
    globals()["t"+globals()["E"+str(i)][1]].Sibilings(globals()["t"+globals()["E"+str(i)][0]],np.uint16(globals()["E"+str(i)][2]),np.uint16(globals()["E"+str(i)][0]))

for i in range(len(Edges)):    
    globals()["t"+str(Edges[i])].start()
    time.sleep(0.2)
for i in range(len(Edges)): 
    globals()["t"+str(Edges[i])].join()    
    time.sleep(0.2)

