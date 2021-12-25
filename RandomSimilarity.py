import logging
import random
import linecache

LOG_FORMAT = f"%(levelname)s %(asctime)s \n\t %(message)s" 
logging.basicConfig(filename = "RandomSimilarity.log", level=logging.INFO, format=LOG_FORMAT, filemode='w') #logger, Log File name:"Operator.log"
logging.info("log file created...")

PATH = 'output.data'
#please set Output_Type = True in Master.py and run that file befor run this method
def RanSim():
    with open(PATH,'r') as file:
        a = {}
        line1 = file.readline().replace('\x00','')
        key , value = line1.split('\t')
        key = key[2:]
        value = [int(i) for i in value.replace(' ','')[1:-2].split(',')]
        a[int(key)] = value
        next(file)    
        for i in range(1,943):
            line1 = file.readline().replace('\x00','')
            key , value = line1.split('\t')
            value = [int(i) for i in value.replace(' ','')[1:-2].split(',')]
            a[int(key)] = value
            next(file)
        Similar_list = []
        for i in range(0,len(a.keys())):
            rand = random.randint(1,len(a))
            while rand==i+1:
                rand = random.randint(1,len(a))
            s = 0
            e = 0
            for j in range(0,len(a[i+1])):
                if a[i+1][j]+a[rand][j]==2:
                    s += 1
                    e += 1
                elif a[i+1][j]+a[rand][j]==1:
                    s += 1
            JC = e/s if s != 0 else 0
            if JC >= 0.5:
                Similar_list.append([i+1,rand])
        with open('Similar_user.data','w') as f:
            for c in range(0,len(Similar_list)):
                f.write(f'{Similar_list[c][0]}\t{Similar_list[c][1]}\n')
            f.close()
            
RanSim()