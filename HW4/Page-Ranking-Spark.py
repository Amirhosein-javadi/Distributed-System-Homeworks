from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
import sys
from operator import add

def computeContribs(urls, rank):
    num_urls = len(urls)
    if num_urls == 1:
        for node in NNodes:
            if (node!=urls[0]):
                yield (node, rank/(N-1)*(1-Beta))
    else:
        for i in range(0,num_urls,2):
            yield (urls[i], rank * urls[i+1]*(1-Beta))
            
    
spark = SparkSession.builder.appName("PiCalculator").getOrCreate()
sc = spark.sparkContext

Filename = sys.argv[1]
Data = sc.textFile(Filename) 
Data = Data.repartition(8) 
Beta = float(sys.argv[2])
Iterations = float(sys.argv[3])
Threshold = float(sys.argv[4])
Data = Data.map(lambda x: x.split(" "))
Data = Data.filter(lambda x: int(x[0])!=int(x[1]))
Data = Data.map(lambda x:  (int(x[0]),(int(x[1]),float(x[2])))).persist(StorageLevel.MEMORY_ONLY)
d1 = Data.map(lambda x: x[0]).distinct().persist(StorageLevel.MEMORY_ONLY)
d2 = Data.map(lambda x: x[1][0]).distinct()
Nodes = d1.union(d2).distinct().persist(StorageLevel.MEMORY_ONLY)
NNodes = Nodes.collect()
N = Nodes.count()
Left_Nodes = Nodes.subtract(d1).persist(StorageLevel.MEMORY_ONLY)
Nodes.unpersist()
d1.unpersist()
sumofweights = Data.map(lambda x: (x[0],x[1][1])).reduceByKey(lambda x,y: x+y)
H = Data.join(sumofweights)
Data.unpersist()
H = H.map(lambda x: (x[0],(x[1][0][0],x[1][0][1]/x[1][1])))
links  = H.reduceByKey(lambda x,y: x+y)
links = links.union(Left_Nodes.map(lambda x:(x,tuple([x])))).persist(StorageLevel.MEMORY_ONLY)
Left_Nodes.unpersist()
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1/N)).persist(StorageLevel.MEMORY_ONLY)
for counter in range(Iterations):
    contribs  = links.join(ranks).flatMap(lambda url_urls_rank:computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    new_ranks = contribs.reduceByKey(add)
    new_ranks =new_ranks.map(lambda x: (x[0],x[1]+Beta/N))
    diff = new_ranks.join(ranks).map(lambda x: abs(x[1][0]-x[1][1])).sum()
    if diff<Threshold:
        break
    ranks = new_ranks.persist(StorageLevel.MEMORY_ONLY)
    
new_ranks.saveAsTextFile("./result")    
spark.stop()


