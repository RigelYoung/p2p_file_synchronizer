#!/usr/bin/python3
#==============================================================================
#usage           :python3 fileSynchronizer.py trackerIP trackerPort
#python_version  :3.5
#==============================================================================

import socket, sys, threading, json,os,time
import os.path
import optparse
from Utils.IOUtils import IOUtils
from Utils.ConversionUtils import ConversionUtils


def validate_ip(s):
    '''
    Arguments:
    s -- dot decimal IP address in string
    Returns:
    True if valid; False otherwise
    '''
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
    '''
    Arguments:
    x -- port number
    Returns:
    True if valid; False, otherwise
    '''
    if not x.isdigit():
        return False
    i = int(x)
    if i < 0 or i > 65535:
            return False
    return True


# 获取文件夹内文件信息
# 可过滤指定后缀类型文件
def get_file_info(blockSize):

    file_info = []
    ignoredTypes = [".exe", ".py", ".pyd", ".dll"]

    entryList = os.scandir(sys.path[0]) #sys.path[0]是调用python解释器的脚本所在的目录
    for entry in entryList:             #os.scandir不会递归到子目录中
        if os.path.isfile(entry):
            isValidFile = True

            for type in ignoredTypes:
                if type in entry.name:
                    isValidFile = False
                    break

            if isValidFile:
                fullName=sys.path[0]+os.sep+entry.name
                fileSize=os.path.getsize(fullName)
                if fileSize < ConversionUtils.megabytes2Bytes(blockSize):
                    blockNum = 0            #不分块的话，为num=0
                else:
                    blockNum = IOUtils.getPartionBlockNum(fullName,blockSize)       #文件分块总数
                    IOUtils.partitionFile(fullName,blockSize)           #进行文件分块      但是应该等真正要传输的时候，再分块

                file_info.append({"name": entry.name,
                                  "mtime": int(os.path.getmtime(entry)), 
                                  "md5":IOUtils.getMD5(fullName), 
                                  "blockNum":blockNum})

    return file_info        #只是获取当前所有文件(去除subfolder)


def check_port_available(check_port):
    if str(check_port) in os.popen("netstat -na").read():
        return False
    return True

def get_next_available_port(initial_port):
    '''
    Arguments:
    initial_port -- the first port to check

    Hint: you can call check_port_available until find one or no port available.
    Return:
    port found to be available; False if no any port is available.
    '''

    #YOUR CODE
    next_available_port = -1

    for port in range(initial_port, 2**16):
        if check_port_available(port):
            next_available_port = port
            break

    return next_available_port


class FileSynchronizer(threading.Thread):
    #这里其实很巧妙。因为我们并不知道ip层会用本机的哪个ip地址，所以我们该peer当服务器时，bind到0.0.0.0，这样监听所有ip地址
    def __init__(self, trackerhost,trackerport,port, host='0.0.0.0'):   

        threading.Thread.__init__(self)
        #Port for serving file requests
        self.port = port 
        self.host = host 

        #Tracker IP/hostname and port
        self.trackerhost = trackerhost 
        self.trackerport = trackerport 

        self.BUFFER_SIZE = 8192    #8k缓冲区大小
        self.blockSize=100         #文件分块大小默认为100M
        self.fileInProcess=set()      #应用于正在请求的文件     因为大文件的话，防止正在请求的文件被多次请求
        self.lock=threading.Lock()      #用于self.fileInProcess

        #与tracker的tcp socket
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.client.settimeout(180)

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

        try:
            self.server.bind((self.host, self.port))
        except socket.error:
            print(('Bind failed %s' % (socket.error)))
            sys.exit()
        self.server.listen(10)

    def exit(self):
        self.server.close()

    #处理其他peer节点发来的file block request，发送文件块
    def process_message(self, conn, addr):          
        timeout = 60.0
        conn.settimeout(timeout)

        print("Client connected with " + addr[0] + ":" + str(addr[1]))
    
        try:
            # Read file name
            requestMsg = ""
            while True:
                requestMsg += conn.recv(self.BUFFER_SIZE).decode()
                if len(requestMsg) < self.BUFFER_SIZE:
                    break
            
            requestMsg=json.loads(requestMsg)
            requestedFileName=requestMsg["name"]
            requestedIdx=requestMsg["blockIdx"]

            if(requestedIdx==0):
                target=sys.path[0]+ os.sep + requestedFileName
            else:
                target=sys.path[0]+ os.sep + 'MEtemp' +os.sep+requestedFileName+ '_PART' + str(requestedIdx)
                #IOUtils.partitionFile(sys.path[0]+ os.sep + requestedFileName,self.blockSize)   #进行文件分块

            #send md5
            md5=IOUtils.getMD5(target)
            conn.send(bytes(md5,"utf-8"))       #32字节
            #print(target+" md5:"+" "+md5)

            sendSize=0

            # send file
            with open(target, "rb") as file:
                while True:
                    content = file.read(self.BUFFER_SIZE)       #因为文件块较大，所以一次只读缓冲区大小，分多次send
                    conn.send(content)  
                    sendSize+=len(content) 
                    #time.sleep(0.1)
                    #if len(content)<self.BUFFER_SIZE:
                    if len(content)==0:
                        break         

        except socket.timeout:
            print("Conn socket timeout!")
        except socket.error as e:
            print('Socket error: %s' % e)
        except json.decoder.JSONDecodeError:
            print('Incorrect format (JSON required)')

        conn.close()
        print(target+" sended "+str(sendSize)+ "bytes")


    def run(self):
        self.client.connect((self.trackerhost,self.trackerport))
        t = threading.Timer(5, self.sync)          #心跳包由sync发送，并进行文件比对，向其他节点要文件     要文件应该多线程并行！！
        t.start()
        print(('Waiting for connections on port %s' % (self.port)))
        while True:
            conn, addr = self.server.accept()       #在这里peer作为server一直监听。收到一个请求，就开一个线程处理，然后继续循环监听。所以发送文件实现了多线程
            threading.Thread(target=self.process_message, args=(conn,addr)).start()


    #给tracker发送心跳包（包含当前节点的所有文件信息），并根据tracker返回的文件信息表去检查需要哪些文件
    def sync(self):
        print(('connect to:' + self.trackerhost, self.trackerport))
        
        localFileList = get_file_info(self.blockSize)
        self.msg = json.dumps({"port": self.port, "files": localFileList}) 
        self.client.send(bytes(self.msg, "utf-8"))

        directory_response_message = ""
        try:
            while True:
                directory_response_message += self.client.recv(self.BUFFER_SIZE).decode()
                if len(directory_response_message) < self.BUFFER_SIZE:
                    self.client.close()                 #收完response，就关闭socket
                    break
        except socket.error as e:
            print('Socket error: %s' % e)


        for filename, fileInfo in json.loads(directory_response_message).items():
            self.lock.acquire()
            if filename in self.fileInProcess:
                self.lock.release()
                continue            #如果这个文件正在被请求，直接略过
            self.lock.release()

            inNeed=True         #是否需要从其他peer索取
            for localFile in localFileList:
                if filename == localFile["name"]:
                    if fileInfo["mtime"] <= localFile["mtime"]:
                        inNeed=False    #已有该文件
                    else:
                        #有同名文件，但是比较旧，那么就先删除，再索取
                        if(os.path.exists(sys.path[0]+os.sep+filename)):
                            os.remove(sys.path[0]+os.sep+filename)
                            if(fileInfo["blockNum"]!=0):
                                IOUtils.delFileBlocks(sys.path[0]+os.sep+filename,fileInfo["blockNum"])
                    break
                
            if inNeed:
                self.lock.acquire()
                self.fileInProcess.add(filename)
                self.lock.release()
                t=threading.Thread(target=self.getFileFromPeer,args=(filename,fileInfo))
                t.start()


        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((self.trackerhost, self.trackerport))       #这里重新和tracker建立连接

        t = threading.Timer(5, self.sync)
        t.start()

    # 索取文件
    def getFileFromPeer(self,filename,fileInfo):                     
        targetPath=target=sys.path[0] + os.sep + filename
        if fileInfo["blockNum"]==0:
            self.getSingleFile(filename,fileInfo)
        else:                                             #多块文件，每个块开一个线程
            tPool=[]            #线程池
            for idx in range(fileInfo["blockNum"]):
                t=threading.Thread(target=self.getSingleFile,args=(filename,fileInfo,idx+1))
                t.start()
                tPool.append(t)
            
            for t in tPool:       #等待各block接收完成
                t.join()   

            #下面合并blocks
            prefix=sys.path[0] + os.sep + 'MEtemp' +os.sep+filename+ '_PART'         
            IOUtils.combineFile(prefix,targetPath,fileInfo["blockNum"])
            os.utime(filename, (os.path.getatime(filename), fileInfo["mtime"]))

        #下面hash校验     如果整个文件不对，重新调用该函数
        md5 = IOUtils.getMD5(targetPath)
        if(md5!=fileInfo["md5"]):
            print(targetPath+" md5校验错误，重新获取文件")
            if(os.path.exists(sys.path[0]+os.sep+filename)):        #先删除错误文件
                os.remove(sys.path[0]+os.sep+filename)
                if(fileInfo["blockNum"]!=0):
                    IOUtils.delFileBlocks(sys.path[0]+os.sep+filename,fileInfo["blockNum"])
            self.getFileFromPeer(filename,fileInfo)

        else:
            self.lock.acquire()
            self.fileInProcess.remove(filename)
            self.lock.release()
            print(targetPath+" md5校验成功")

            #删除分块文件
            # if(fileInfo["blockNum"]!=0):
            #     IOUtils.delFileBlocks(sys.path[0]+os.sep+filename,fileInfo["blockNum"])

    
    def getSingleFile(self,filename,fileInfo,idx=0):                   
        fileSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        fileSocket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        fileSocket.settimeout(60.0)
        fileSocket.connect((fileInfo["ip"], fileInfo["port"]))
        report={"name":filename,"blockIdx":idx}
        fileSocket.send(bytes(json.dumps(report), 'utf-8')) 

        if idx==0:
            target=sys.path[0] + os.sep + filename
        else:
            target=sys.path[0] + os.sep + 'MEtemp' +os.sep+filename+ '_PART' + str(idx)

        try:
            md5=fileSocket.recv(32).decode()   
            print(target+" md5:"+" "+md5)     

            if not os.path.exists(sys.path[0] + os.sep + 'MEtemp'):
                os.mkdir(sys.path[0] + os.sep + 'MEtemp')      #创建缓存文件夹
            
            recvSize=0
            
            with open(target, "wb") as file:
                while True:
                    content = fileSocket.recv(self.BUFFER_SIZE)
                    file.write(content)
                    recvSize+=len(content)

                    #if len(content) < self.BUFFER_SIZE:
                    if len(content)==0:
                        break
                    #time.sleep(0.1)

        except socket.error as e:
            print('Socket error: %s' % e)
        fileSocket.close()

        print(target+" received "+str(recvSize)+ "bytes")

        if idx==0:
            os.utime(filename, (os.path.getatime(filename), fileInfo["mtime"]))
        
        if(md5!=IOUtils.getMD5(target)):
            #print(target+" 文件md5:"+" "+IOUtils.getMD5(target)) 
            print(target+" md5校验错误，重新获取文件")
            if(os.path.exists(target)):
                os.remove(target)
            self.getSingleFile(filename,fileInfo,idx)       #重新调用函数
        


if __name__ == '__main__':
    # parse command line arguments  命令行传入tracker的ip:port
    parser = optparse.OptionParser(usage="%prog ServerIP ServerPort")
    options, args = parser.parse_args()
    if len(args) < 1:
        parser.error("No ServerIP and ServerPort")
    elif len(args) < 2:
        parser.error("No ServerIP or ServerPort")
    else:
        if validate_ip(args[0]) and validate_port(args[1]):
            tracker_ip = args[0]
            tracker_port = int(args[1])

        else:
            parser.error("Invalid ServerIP or ServerPort")

    synchronizer_port = get_next_available_port(8000)       #找到一个空闲的端口
    synchronizer_thread = FileSynchronizer(tracker_ip,tracker_port,synchronizer_port)
    synchronizer_thread.start()
