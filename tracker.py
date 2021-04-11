#!/usr/bin/python3
#==============================================================================
#usage           :python3 tracker.py trackerIP trackerPort
#python_version  :3.5
#==============================================================================
import socket, sys, threading, json, time, optparse, os

def validate_ip(s):
    a = s.split('.')
    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True


def validate_port(x):
    if not x.isdigit():
        return False
    i = int(x)
    if i < 0 or i > 65535:
            return False
    return True


class Tracker(threading.Thread):
    def __init__(self, port, host='0.0.0.0'):
        threading.Thread.__init__(self)
        self.port = port #tracker port
        self.host = host #tracker IP address
        self.BUFFER_SIZE = 8192
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #socket to accept tcp connections from peers

         # 记录每个peer (ip, port, exp time)  using a dictionary
         # (ip,port) as key          注意，这里存的port是peer server的端口，并不是tracker连接端口
        self.users = {}

        # 只有文件的最新时间被记录
        # {'ip':,'port':,'mtime':,'md5':,'blockNum':}
        # 字典嵌套  key=filename，value=文件信息字典
        self.files = {}

        self.userLock = threading.Lock()
        self.fileLock = threading.Lock()
        try:
            #Bind to address and port
            self.server.bind((self.host, self.port))
        except socket.error:
            print(('Bind failed %s' % (socket.error)))
            sys.exit()

        #listen for connections
        self.server.listen(6)

    def check_user(self):
        deleteUser = []
        deleteFile = []
        self.userLock.acquire()
        for user, TTL in self.users.items():        #遍历user字典
            if time.perf_counter() - TTL > 180: 
                self.fileLock.acquire()
                for file, info in self.files.items():
                    if info['ip']==user[0] and info['port'] == user[1]:
                        deleteFile.append(file)               
                self.fileLock.release()
            
                deleteUser.append(user)
        self.userLock.release()    

        if deleteUser:
            self.userLock.acquire()
            for user in deleteUser:
                del self.users[user]
            self.userLock.release()
        
        if deleteFile:
            self.fileLock.acquire()
            for file in deleteFile:
                del self.files[file]
            self.fileLock.release()

        #间隔20s检查
        t = threading.Timer(20, self.check_user)
        t.start()

    def exit(self):
        self.server.close()

    def run(self):
        # start the timer to check if peers are alive or not
        t = threading.Timer(20, self.check_user)        #这里设置间隔20s
        t.start()

        print(('Waiting for connections on port %s' % (self.port)))
        while True:             #这里tracker类已经在子线程上了。 死循环，去监听
            conn, addr = self.server.accept()
            #收到请求，就开线程去处理
            threading.Thread(target=self.process_messages, args=(conn, addr)).start()


    def process_messages(self, conn, addr):
        conn.settimeout(180.0)
        print(('Client connected with ' + addr[0] + ':' + str(addr[1])))
        try:
            #receiving data from a peer
            data = ''
            while True:             
                part = conn.recv(self.BUFFER_SIZE).decode()     #将utf-8编码转成当前程序使用的编码，默认也是unicode
                data = data + part                              #把conn当成一个IO流，可以一直读取   实际上就是内核态io buffer
                if len(part) < self.BUFFER_SIZE:
                    break

            try:
                #将json解析成python对象，这里为字典
                data_dic = json.loads(data)     
            except json.decoder.JSONDecodeError:
                print('Incorrect format (JSON required)')
       
            # Keepalive
            self.userLock.acquire()
            self.users[(addr[0], data_dic['port'])] = time.perf_counter()         #心跳时间都是tracker上的时间，只要文件时间戳是peer发过来的时间
            self.userLock.release() 

            # Check if new file
            for peerFile in data_dic['files']:
                newFile = True
                oldFile = ''
                for file in self.files:             #遍历字典的key,即文件名
                    if file == peerFile['name']:
                        newFile = False
                        oldFile = file
                        break

                #记录新文件
                if newFile:
                    self.fileLock.acquire()
                    self.files[peerFile['name']] = {'ip' : addr[0], 
                                                    'mtime' : peerFile['mtime'], 
                                                    'port' : data_dic['port'], 
                                                    'md5': peerFile['md5'],
                                                    'blockNum': peerFile['blockNum']}
                    self.fileLock.release()
                #更新旧文件信息
                elif self.files[oldFile]['mtime'] > peerFile['mtime']:      #如果这是同名的更新的文件
                    self.fileLock.acquire()
                    self.files[oldFile]['ip'] = addr[0]
                    self.files[oldFile]['mtime'] = peerFile['mtime']
                    self.files[oldFile]['port'] = data_dic['port']
                    self.files[oldFile]['md5'] = peerFile['md5']
                    self.files[oldFile]['blockNum'] = peerFile['blockNum']
                    self.fileLock.release()

            # Send directory response message
            conn.send(bytes(json.dumps(self.files), 'utf-8'))         
            
        except socket.timeout as e:     
            #tcp连接超时，关闭socket连接，并结束线程
            print("connection timeout!")

        conn.close() # Close

if __name__ == '__main__':
    parser = optparse.OptionParser(usage="%prog ServerIP ServerPort")
    options, args = parser.parse_args()
    if len(args) < 1:
        parser.error("No ServerIP and ServerPort")
    elif len(args) < 2:
        parser.error("No ServerIP or ServerPort")
    else:
        if validate_ip(args[0]) and validate_port(args[1]):
            server_ip = args[0]
            server_port = int(args[1])
        else:
            parser.error("Invalid ServerIP or ServerPort")
    tracker = Tracker(server_port,server_ip)
    tracker.start()
