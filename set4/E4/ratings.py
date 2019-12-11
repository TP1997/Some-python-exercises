
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import os

class BookRatings(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def mapper(self, _, line):
        fname = os.getenv('mapreduce_map_input_file').split('//')[1]
        if fname=="BX-Users.csv":
            (uid,loc,age)=line.split(";")
            (uid,loc,age)=(uid,loc.split(", ")[-1],age)
            yield((loc,uid),1)
        else:
            try:
                #line=line.decode('utf_8')
                (uid,isbn,rating)=line.split(";")
                yield(uid, float(rating))
            except ValueError:
                print(2)
                pass
    
    def combiner(self, key, val_itr):
        if type(key)==tuple:
            yield(key, sum(val_itr))
        else:
            vals=list(val_itr)
            ln=len(vals)
            tot=sum(map(float,vals))
            yield(key, tot/ln)
            
    def reducer(self, key, val_itr):
        vals=list(val_itr)
        ln=len(vals)
        tot=sum(map(int,vals))
        yield(None, tot/ln)
    
    
    
if __name__ == '__main__':
    BookRatings.run() 

