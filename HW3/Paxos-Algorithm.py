from threading import Thread
import socket
socket.setdefaulttimeout(1)
from queue import Queue
import json
import time
import numpy as np


class Message:
    def __init__(self,Nid,Type, Value):
        self.Nid         = Nid    
        self.Type         = Type      
        self.Value        = Value  
        
class MyThread(Thread):
    def __init__(self,uid,numofnodes,t1,t2,t3):
        super().__init__(name=f'Process {uid} Thread')        
        self.UID = uid
        self.SearchRespondtime = t1
        self.LeaderRespondtime = t2
        self.ValueRespondtime  = t3
        self.N = numofnodes
        self.Channel = {}
        self.ChannelDelay = {}
        self.State = None  # responder,Leader,decide
        self.Awake = True  #False the process decides
        self.Receiving    = Receive(self)   # Receiving mechanism
        self.Sending      = {}  # Sending mechanism
        self.DecideValue = 0
        self.LeaderId = 0
        self.BigestProposedValue = 0
        self.BestProposer  = None
        self.Neighbour = {}
        self.Timer = Timer(self)
    def SetChannel(self,channeluid,channeldelay):
        channeluid = np.int16(channeluid)
        channeldelay = np.float(channeldelay)
        self.Channel[channeluid] = Queue()
        self.ChannelDelay[channeluid] = channeldelay
        self.Sending[channeluid] = Send(self,channeluid,channeldelay)
    def SetNeighbour(self,channeluid,neighbour):
        self.Neighbour[channeluid] = neighbour
    def run(self):
        self.State = 'responder'        
        self.Timer.start()        
        self.Receiving.start()
        for items in self.Sending.keys():
            self.Sending[items].start() 
        self.Timer.join()      
        self.Receiving.join() 
        for items in self.Sending.keys():        
            self.Sending[items].join()   

class Timer(Thread):
    def __init__(self,Process):  
        self.Process = Process 
        super().__init__(name=f'Timer {Process.UID} Thread')
    def run(self):
        while self.Process.Awake:
            start = time.time()   
            period = self.Process.SearchRespondtime
            while self.Process.State == 'responder':
                if self.Process.Awake == False:
                    break
                end = time.time()
                if (end-start)>period:
                    self.Process.State = 'Leader'
            start = time.time()       
            period = self.Process.LeaderRespondtime
            while self.Process.State == 'Leader':
                if self.Process.Awake == False:
                    break                
                end = time.time()
                if (end-start)>period:                 
                    self.Process.State = 'responder'  # if the node get enough ok it goes to decide state
            start = time.time()       
            period = self.Process.ValueRespondtime   
            while self.Process.State == 'decide':
                if self.Process.Awake == False:
                    break                
                end = time.time()
                if (end-start)>period:   
                    self.Process.State = 'responder'  

class Receive(Thread):
    def __init__(self, Process):
        self.Process    = Process
        super().__init__(name=f'Receive {Process.UID} Thread')    
        self.Socket      = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
        self.Ip          = socket.gethostname()
        self.Port        = 0 
        self.Socket.bind((self.Ip,self.Port))  
        self.Address     = self.Socket.getsockname() 
        self.Leaderflag  = True
        self.Valueflag   = True
    def run(self):
        while(self.Process.Awake): 
            while self.Process.State=='responder':
                if self.Process.Awake == False:
                    break
                self.Socket.listen(5) 
                try:
                    connection,address   = self.Socket.accept()    # wait until one socket make a connection
                    ReceivedMessage      = Message(**json.loads(connection.recv(1024)))
                    # print(f'{ReceivedMessage.Type}')
                    connection.close()
                    if ReceivedMessage.Type == 'POTENTIAL_LEADER':
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        if ReceivedMessage.Value > self.Process.LeaderId:
                            self.Process.LeaderId = ReceivedMessage.Value
                            if self.Process.BestProposer == None:
                                SenderMessage = Message(self.Process.UID,'POTENTIAL_LEADER_ACK',"0 , -1")
                            else:
                                SenderMessage = Message(self.Process.UID,'POTENTIAL_LEADER_ACK',f'{self.Process.BestProposer} , {self.Process.BigestProposedValue}')   
                            self.Process.Channel[ReceivedMessage.Nid].put((SenderMessage,self.Process.Neighbour[ReceivedMessage.Nid].Receiving.Address))
                                                          
                    elif ReceivedMessage.Type == 'V_DECIDE':  
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        self.Process.DecideValue = ReceivedMessage.Value
                        self.Process.Awake = False
                        break
                    
                    elif ReceivedMessage.Type == 'V_PROPOSE': 
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        message = ReceivedMessage.Value
                        numbers = []
                        for word in message.split():
                            if word.isdigit():
                                numbers.append(int(word))
                        if numbers[0]>=self.Process.LeaderId:
                            self.Process.BigestProposedValue = numbers[1]
                            self.Process.BestProposer = numbers[0]        
                            SenderMessage = Message(self.Process.UID,'V_PROPOSE_ACK',-1)   
                            self.Process.Channel[ReceivedMessage.Nid].put((SenderMessage,self.Process.Neighbour[ReceivedMessage.Nid].Receiving.Address))                       
                except socket.timeout:
                    pass


            counter = 0  
            self.Leaderflag = True
            while self.Process.State=='Leader':
                if self.Leaderflag == True:
                    self.Leaderflag = False
                    self.Process.LeaderId = self.Process.LeaderId + 1
                    SenderMessage = Message(self.Process.UID,'POTENTIAL_LEADER',self.Process.LeaderId) 
                    for addrs in self.Process.Neighbour.keys():
                        self.Process.Channel[addrs].put((SenderMessage,self.Process.Neighbour[addrs].Receiving.Address))            
                self.Socket.listen(5) 
                try:
                    connection,address   = self.Socket.accept()
                    ReceivedMessage      = Message(**json.loads(connection.recv(1024))) 
                    connection.close()
                    if ReceivedMessage.Type == 'POTENTIAL_LEADER_ACK':
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')                        
                        message = ReceivedMessage.Value
                        numbers = []
                        for word in message.split():
                            if word.isdigit():
                                numbers.append(int(word))     
                        counter = counter + 1
                        if numbers[0] != 0 :
                            if numbers[1] > self.Process.BigestProposedValue:
                                self.Process.BigestProposedValue = numbers[1]
                                self.Process.BestProposer = numbers[0]     
                        if counter>self.Process.N//2:
                            self.Process.State = 'decide'
                            break 
                    elif ReceivedMessage.Type == 'POTENTIAL_LEADER':
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        if ReceivedMessage.Value > self.Process.LeaderId:
                            self.Process.LeaderId = ReceivedMessage.Value
                            self.Process.State = 'responder'   
                            if self.Process.BestProposer == None:
                                SenderMessage = Message(self.Process.UID,'POTENTIAL_LEADER_ACK',"0 , -1")
                            else:
                                SenderMessage = Message(self.Process.UID,'POTENTIAL_LEADER_ACK',f'{self.Process.BestProposer} , {self.Process.BigestProposedValue}')   
                            self.Process.Channel[ReceivedMessage.Nid].put((SenderMessage,self.Process.Neighbour[ReceivedMessage.Nid].Receiving.Address))
                            break                                                                                 
                    elif ReceivedMessage.Type == 'V_PROPOSE': 
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        message = ReceivedMessage.Value
                        numbers = []
                        for word in message.split():
                            if word.isdigit():
                                numbers.append(int(word))
                        if numbers[0]>=self.Process.LeaderId:
                            self.Process.BigestProposedValue = numbers[1]
                            self.Process.BestProposer = numbers[0]        
                            SenderMessage = Message(self.Process.UID,'V_PROPOSE_ACK',-1)   
                            self.Process.Channel[ReceivedMessage.Nid].put((SenderMessage,self.Process.Neighbour[ReceivedMessage.Nid].Receiving.Address))                       
                    
                    elif ReceivedMessage.Type == 'V_DECIDE':   
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        self.Process.DecideValue = ReceivedMessage.Value
                        self.Process.Awake = False
                        break                                                                
                
                except socket.timeout:
                    pass
                
                
            counter = 0   
            self.Valueflag = True
            while self.Process.State=='decide':                
                if self.Valueflag == True:           
                    self.Valueflag = False
                    if self.Process.BigestProposedValue == 0:
                        self.Process.BigestProposedValue = self.Process.LeaderId*self.Process.N
                    SenderMessage = Message(self.Process.UID,'V_PROPOSE',f'{self.Process.LeaderId} , {self.Process.BigestProposedValue}')  
                    for addrs in self.Process.Neighbour.keys():
                        self.Process.Channel[addrs].put((SenderMessage,self.Process.Neighbour[addrs].Receiving.Address))                
                        
                self.Socket.listen(5) 
                try:
                    connection,address   = self.Socket.accept()
                    ReceivedMessage      = Message(**json.loads(connection.recv(1024))) 
                    connection.close()
                    if ReceivedMessage.Type == 'V_PROPOSE_ACK':
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')                      
                        counter = counter + 1
                        if counter>self.Process.N//2:
                            SenderMessage = Message(self.Process.UID,'V_DECIDE',f'{self.Process.BigestProposedValue}') 
                            for addrs in self.Process.Neighbour.keys():
                                self.Process.Channel[ReceivedMessage.Nid].put((SenderMessage,self.Process.Neighbour[addrs].Receiving.Address))                                
                            self.Process.DecideValue= self.Process.BigestProposedValue
                            self.Process.Awake = False
                            break  
                        
                    elif ReceivedMessage.Type == 'POTENTIAL_LEADER':
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        if ReceivedMessage.Value > self.Process.LeaderId:
                            self.Process.LeaderId = ReceivedMessage.Value
                            self.Process.State = 'responder'       
                            if self.Process.BestProposer == None:
                                SenderMessage = Message(self.Process.UID,'POTENTIAL_LEADER_ACK',"0 , -1")
                            else:
                                SenderMessage = Message(self.Process.UID,'POTENTIAL_LEADER_ACK',f'{self.Process.BestProposer} , {self.Process.BigestProposedValue}')   
                            self.Process.Channel[ReceivedMessage.Nid].put((SenderMessage,self.Process.Neighbour[ReceivedMessage.Nid].Receiving.Address))
                            break               
                    elif ReceivedMessage.Type == 'V_Propose': 
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        message = ReceivedMessage.Value
                        numbers = []
                        for word in message.split():
                            if word.isdigit():
                                numbers.append(int(word))
                        if numbers[0]>=self.Process.LeaderId:
                            self.Process.BigestProposedValue = numbers[1]
                            self.Process.BestProposer = numbers[0]        
                            SenderMessage = Message(self.Process.UID,'V_PROPOSE_ACK',-1)   
                            self.Process.Channel[ReceivedMessage.Nid].put((SenderMessage,self.Process.Neighbour[ReceivedMessage.Nid].Receiving.Address))                       
                    
                    elif ReceivedMessage.Type == 'V_DECIDE':   
                        print(f'node {self.Process.UID}: {ReceivedMessage.Nid} {ReceivedMessage.Type} {ReceivedMessage.Value}')
                        self.Process.DecideValue = ReceivedMessage.Value
                        self.Process.Awake = False
                        break       
                    
                    
                except socket.timeout:
                    pass                
                
                
                
                
                
                
                
class Send(Thread):
    def __init__(self, Process,channeluid,delay):
        self.Process    = Process
        super().__init__(name=f'Send {Process.UID} Thread')
        self.Socket     = None
        self.Address    = None
        self.delay = delay
        self.channeluid = channeluid        
    def run(self):
        while self.Process.Awake:
            while not self.Process.Channel[self.channeluid].empty():
                self.Socket     = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.Ip         = socket.gethostname()
                self.Port       = 0 
                self.Socket.bind((self.Ip,self.Port))
                self.Address    = self.Socket.getsockname()
                message,addr   = self.Process.Channel[self.channeluid].get()
                try:  
                    temp = self.Socket.connect_ex(addr)
                    while temp != 0:
                        temp = self.Socket.connect_ex(addr)
                    time.sleep(self.delay/2)
                    self.Socket.send(json.dumps(message.__dict__).encode("utf-8"))    
                    time.sleep(self.delay/2)
                except: 
                    pass                
                finally:
                    self.Socket.close() 

# n = np.uint16(input())
# n = 3
# t1 =  MyThread(1,n,10,6,5)  
# t1.SetChannel(2,1)
# t1.SetChannel(3,1.2)
# t2 =  MyThread(2,n,14,5,5)  
# t2.SetChannel(1,1.5)
# t2.SetChannel(3,1)
# t3 =  MyThread(3,n,18,6,6)  
# t3.SetChannel(1,1)
# t3.SetChannel(2,2.5)

# t1.SetNeighbour(2,t2)
# t1.SetNeighbour(3,t3)
# t2.SetNeighbour(1,t1)
# t2.SetNeighbour(3,t3)
# t3.SetNeighbour(1,t1)
# t3.SetNeighbour(2,t2)

# t1.start() 
# t2.start() 
# t3.start() 
# t1.join() 
# t2.join() 
# t3.join() 
n = np.uint16(input())
for i in range(1,n+1):
    globals()["uid"+str(i)],time1,time2,time3 = input().split(" ")
    globals()["uid"+str(i)] = int(globals()["uid"+str(i)])
    time1 = float(time1)
    time2 = float(time2)
    time3 = float(time3) 
    globals()["t"+str(globals()["uid"+str(i)])] = MyThread(globals()["uid"+str(i)],n,time1,time2,time3)  
    for j in range(1,n):  
        channeluid,channeldelay = input().split(" ")
        channeluid =  int(channeluid)
        channeldelay =  float(channeldelay)
        globals()["t"+str(globals()["uid"+str(i)])].SetChannel(channeluid,channeldelay)
for i in range(1,n+1): 
    for j in range(1,i):
        globals()["t"+str(globals()["uid"+str(i)])].SetNeighbour(np.int16(globals()["uid"+str(j)]),globals()["t"+str(globals()["uid"+str(j)])])
    for j in range(i+1,n+1):
        globals()["t"+str(globals()["uid"+str(i)])].SetNeighbour(np.int16(globals()["uid"+str(j)]),globals()["t"+str(globals()["uid"+str(j)])])
        

for i in range(1,n+1):     
    globals()["t"+str(globals()["uid"+str(i)])].start() 
for i in range(1,n+1):     
    globals()["t"+str(globals()["uid"+str(i)])].join() 
  

           
       
    
