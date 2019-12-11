
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep
import os

class CalcTweets(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer2)
        ]
    
    def mapper(self, _, line):
        try:
            timedata=line.split(',')[2]
            date=timedata.split()[0]
            year=date.split('-')[2]
            hour=timedata.split()[1].split(':')[0]
            yield((year,hour),1)         
        except:
            pass
    
    def combiner(self, yr_hr, cnt_itr):
        yield(yr_hr, sum(cnt_itr))
        
    def reducer(self, yr_hr, cnt_itr):
        yield(yr_hr[0], (yr_hr[1], sum(cnt_itr)))
        
        
        
    def reducer2(self, yr, hr_cnt_itr):
        res=yr
        tot_sum=0
            
        hr_cnt=dict.fromkeys(range(24),0)
        for (hour, tweetcount) in hr_cnt_itr:
            hr_cnt[int(hour)]=tweetcount
            #hr_cnt.append((hour, tweetcount))
            tot_sum+=tweetcount
            
        for hour in hr_cnt.keys():
            tweetcount=hr_cnt[hour]
            per=100*tweetcount/tot_sum
            res+= "{:2.0f} ".format(round(per))
            
        #for (hour, tweetcount) in hr_cnt:
            #res+= " {:2.0f}".format(100*tweetcount/tot_sum)
            #res+= f' {round(100*tweetcount/tot_sum,2)}'
        
        if(yr==2009):
            res+=f' avg:{round(tot_sum/241,1)}'
        elif(yr==2019):
            res+=f' avg:{round(tot_sum/326,1)}'
        else:
            res+=f' avg:{round(tot_sum/365,1)}'
        
        yield(None, res)
        
        
if __name__ == '__main__':
    CalcTweets.run()
