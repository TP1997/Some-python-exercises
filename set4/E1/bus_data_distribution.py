
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import sys

class BusDataDist(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def mapper_init(self):
        self.intervals=set(map(str, list(range(0,24))))
        self.found_intervals=set()
        
        
    def mapper(self, key, line):
        if line != ';;;;;;;':
            time=line.split(';')[1]
            try:
                (interval,_,_)=time.split(':')
                interval = interval[1] if interval[0]=='0' else interval 
                self.found_intervals.add(interval)
                yield(interval, 1)
            except ValueError:
                pass
    
    # Handle hour intervals which didn't occur in the data.
    def mapper_final(self):
        missed_intervals=self.intervals.difference(self.found_intervals)
        for interval in missed_intervals:
            yield(interval, 0)
    
    def combiner(self, intr, cnt_itr):
        yield(intr, sum(cnt_itr))
               
    def reducer(self, intr, cnt_itr):
        yield(None, f'{intr};{sum(cnt_itr)}')
        
    
        
if __name__ == '__main__':
    BusDataDist.run()
