
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class PrimeAverage2(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    def mapper(self, _, line):
        primes=line.split()
        try:
            fprimes=list(map(float, primes))
            if len(fprimes)>0:
                yield(None, (sum(fprimes), 8))
        except ValueError:
            pass
    
    def combiner(self, _, sum_cnt_itr):
        totsum=0
        totcnt=0
        for sum_, cnt in sum_cnt_itr:
            totsum += sum_
            totcnt += cnt
            
        yield(None, (totsum, totcnt))
        
    def reducer(self, _, sum_cnt_itr):
        totsum=0
        totcnt=0
        for sum_, cnt in sum_cnt_itr:
            totsum += sum_
            totcnt += cnt
        
        avg=totsum/totcnt
        yield(None, str(avg))
        
        
if __name__ == '__main__':
    PrimeAverage2.run()        
