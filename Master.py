import logging #log for debuging, info and error
from mrjob.job import MRJob #user for map reduce
import re #user for split
import linecache #for giving line
import os #for rename data file (utf-8)

LOG_FORMAT = f"%(levelname)s %(asctime)s \n\t %(message)s" 
logging.basicConfig(filename = "Master.log", level=logging.INFO, format=LOG_FORMAT, filemode='w') #logger, Log File name:"Operator.log"
logging.info("log file created...")
try:
    os.rename(r'u.item',r'item.data')
    os.rename(r'u.genre',r'genre.data')
except:
    pass

Output_Type = True #True: result by gener matrix (row list of 0 or 1)     False: result by Genre Name
#please set Output_Type = True and run this file befor run RanSim() in RandomSimilarity.py

class MRSimilarityFind(MRJob):
    def mapper(self, _, line):
        s = re.split(r'\t+', line.rstrip('\t'))
        yield int(s[0]), [int(s[1]), int(s[2])]

    def reducer(self, key, values):
        Sum_Genre_Rate = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        Number_of_Rate = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        Favourite_Genre = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        v = list(values)

        for inner_list in v:
            genre_item = linecache.getline('item.data', inner_list[0]).split('|')[5:]
            genre_item[-1] = genre_item[-1][0]
            # genre_item =  [ True if x == '1' else False for x in genre_item  ] #change item to True False
            index = (i for i, e in enumerate(genre_item) if e == '1')
            while True:
                try:
                    indx = next(index)
                    Sum_Genre_Rate[indx] += inner_list[1]
                    Number_of_Rate[indx] += 1
                except:
                    break

        for j in range(0,len(Number_of_Rate)):
            if ((Sum_Genre_Rate[j]/Number_of_Rate[j]) if Number_of_Rate[j] != 0 else 0) >= 3:
                Favourite_Genre[j] = 1

        if Output_Type == True:
            yield key, Favourite_Genre
        else:
            Favourite_String = ''
            index = (i for i, e in enumerate(Favourite_Genre) if e == 1)
            while True:
                try:
                    indx = next(index)
                    Favourite_String += f"  {linecache.getline('genre.data',indx+1)}".split('|')[0]
                except:
                    break
            yield key, Favourite_String


if __name__ == '__main__':
    MRSimilarityFind.run()