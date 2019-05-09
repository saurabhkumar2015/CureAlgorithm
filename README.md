##	Cure Algorithm

There are three main steps performed in implementing cure algorithm with spark.

## 1) Sample and repartition data:
     a) Use chernoff ratio to sample data and reduce size. 
        Chernoff formula:
        Number of samples needed = 5050 * Math.log(2/(1-confidence%)) 

     b) After sampling, equally re-distribute data points in partitions.

     b) Convert each point to a cluster.
    
## 2) Hierarchical clustering:
     a) Create kd Tree from all points. Special kd Tree function “closestPointOfOtherCluster”      returns the closest point of the               nearest cluster and not the point in the same cluster.
     b) Create ClusterMinHeap from all clusters.
     c) Do Hierarchical clustering and merge clusters within a partition till we have k clusters per partition.
     d) Drop all points and select only m representatives. Dropping all points makes data light-weight.
     
## 3) Spark collect and Merge clusters using representatives:

     a) Collect k clusters from each of the e partitions. Every cluster has m representatives.
     b) Merge the k*e clusters using representatives to form our final k clusters 
     c) Reload data set again say data has n partitions. Broadcast and send each of the final k clusters kd Tree to each of n partitions           of the newly loaded dataset.
     d) Assign all points in each of n partitions to the nearest of the final k clusters using the m representatives.
