
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class PrimeAverage(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def mapper(self, _, line):
        primes=line.split()
        try:
            fprimes=list(map(float, primes))
            yield(None, (sum(fprimes)/8, 8))
        except ValueError:
            pass
        
    def combiner(self,n_key,part_avg_itr):
        cnt_=0
        sum_=0
        for avg, cnt in part_avg_itr:
            sum_+=avg*cnt
            cnt_+=cnt
        yield(None, sum_/cnt_)
        
    def reducer(self, n_key, part_avg_itr):
        cnt_=0
        sum_=0
        for avg, cnt in part_avg_itr:
            sum_+=avg*cnt
            cnt_+=cnt
        yield(None, sum_/cnt_)
        
