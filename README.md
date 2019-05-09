##	Cure Algorithm

There are three main steps performed in implementing cure algorithm with spark.



## 1) Sample and repartition data:
Use chernoff ratio to sample data and reduce size. 
Chernoff formula:
Number of samples needed = 5050 * Math.log(2/(1-confidence%)) 

     b)   After sampling, equally re-distribute data points in partitions.

     b)   Convert each point to a cluster.
