from pyspark.sql import SparkSession
from operator import add

def read_data(hdfs_folder):
    data = sc.parallelize([])
    data = sc.textFile(hdfs_folder)
    return data

def clean_data(line):
    tabs = line.split(',')
    if len(tabs) == 7:
        return tabs

def calc_year_avg(year_tpl):
    year = year_tpl[0]
    count = year_tpl[1]
    if year == "2019":
        return (year,round(count / 326,1))
    elif year == "2009":
        return (year,round(count / 241,1))
    else:
        return (year,round(count / 365,1))


spark = SparkSession\
        .builder\
        .appName("SparkTweets")\
        .getOrCreate()
sc = spark.sparkContext

tweets=read_data("hdfs:/user/group19/trump_tweets.csv")

tweets=tweets.flatMap(lambda line: line.split('\n'))
tweets=tweets.filter(lambda line: line!='source,text,created_at,retweet_count,favorite_count,is_retweet,id_str')
tweets=tweets.map(clean_data)
tweets=tweets.filter(lambda x: x is not None)

tweets=tweets.map(lambda line: (line[2], line[-1]))
tweets=tweets.map(lambda line: (line[0].split()[0].split('-')[-1], line[0].split()[1].split(':')[0]))

tot_hours=tweets.keys().map(lambda x: (x, 1)).reduceByKey(add)

years = tweets.keys().distinct().sortByKey()#.map(lambda x : (x))

hours = tweets.map(lambda x: x[1]).distinct().sortByKey()

def map_hours(hour_list):
    n_list = []
    for h in hour_list:
        n_list.append( (int(h), 1) )
    return n_list

def reduce_hours(hour_list):
    helper_dict = dict.fromkeys(range(24), 0)

    for h in hour_list:
        helper_dict[ h[0] ] += 1

    return list(helper_dict.items())

def combine(pct_avg_lst):
    pcts=pct_avg_lst[0]
    pcts.append(pct_avg_lst[1])
    return pcts

def outputformat(row):
    res=row[0]

    for idx in range(len(row[1])-1):
        res+="{:2.0f} ".format(round(row[1][idx]))

    res+=" {0}".format(row[1][-1])
    return res

hour_dist = tweets.map(lambda kv_tuple: (kv_tuple[0], [kv_tuple[1]])).reduceByKey(lambda a,b: a+b).mapValues(map_hours).mapValues(reduce_hours)

hour_dist=hour_dist.join(tot_hours)
pct_dist=hour_dist.mapValues(lambda x: [100*cnt/x[1] for hr, cnt in x[0]])
avgs=tot_hours.map(calc_year_avg)

res_dist=pct_dist.join(avgs)
res_dist=res_dist.mapValues(combine)

res_dist.map(outputformat).saveAsTextFile("hdfs:/user/group19/sparktweets")

writable_data=res_dist.collect()

with open("sparktweets.txt", "w") as f:
    for row in writable_data:
        res=row[0]
        for data in row[1][:-1]:
            res+= "{:2.0f} ".format(round(data))
        res+=" {0}\n".format(row[1][-1])
        f.write(res)

    f.close()
