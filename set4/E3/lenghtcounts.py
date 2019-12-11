
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep
from collections import defaultdict
from json import dumps
import os

class LenCounts(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer2)
        ]
    
    def mapper(self, _, line):
        file_name = os.getenv('mapreduce_map_input_file')
        words=line.split()
        for word in words:
            yield((file_name, len(word)), 1)
                    
    def combiner(self, fn_len, cnt_itr):
        yield(fn_len, sum(cnt_itr))
    
    def reducer(self, fn_len, cnt_itr):
        yield(fn_len[0], (fn_len[1], sum(cnt_itr)))
        
    def reducer2(self, fn, len_cnt_itr):
        json_dict={}
        json_dict["file"]=fn.split('//')[1]

        for data in len_cnt_itr:
            ln=data[0]
            cnt=data[1]
            json_dict[str(ln)]=cnt
            
        yield(None, dumps(json_dict))
        
        
        
if __name__ == '__main__':
    LenCounts.run() 
