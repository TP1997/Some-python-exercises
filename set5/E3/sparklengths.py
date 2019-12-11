from pyspark.sql import SparkSession
from operator import add
from pyspark.sql.functions import input_file_name
from collections import Counter
import json

spark = SparkSession\
        .builder\
        .appName("SparkLengths")\
        .getOrCreate()
sc = spark.sparkContext

def read_data(hdfs_folder):
    data = sc.parallelize([])
    data = sc.wholeTextFiles(hdfs_folder)
    return data

words=read_data("hdfs:/bigdata/americana/americana.txt*")

words=words.flatMapValues(lambda line: line.split()).map(lambda x: (x[0].split('/')[-1], len(x[1])))

wordcount=words.map(lambda x: ((x[0],x[1]),1)).reduceByKey(add)
wordcount=wordcount.map(lambda x: (x[0][0],[(x[0][1],x[1])])).reduceByKey(lambda a,b: a+b)

final_wc=wordcount.collect()


def create_json(data):
	
	json_obj_lst=[]
	for fn_lst in data:
		json_obj={}
		json_obj["file"]=fn_lst[0]
		for elem in fn_lst[1]:
			json_obj[str(elem[0])]=elem[1]

		json_obj_lst.append(json_obj)

	return json_obj_lst


json_list=create_json(final_wc)
json_string=json.dumps(json_list)
with open("lencounts.json", "w") as f:
    f.write(json_string)

