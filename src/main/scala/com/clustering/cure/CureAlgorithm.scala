package com.clustering.cure

import com.clustering.kdtree.{KDNode, KDTree}
import com.clustering.point.Point
import com.clustering.heap.ClusterMinHeap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

/**
  * @author ${user.name}
  *
  *  To start a CURE algorithm in spark 5 attributes are required.
  *
  *  "c" confidence lies between and not including 0 and 1. 0.99 means 99% confidence that my sampling represents the true picture
  *  "k" for number of clusters
  *  "m" for number of representatives
  *  "s" for shrinking factor
  *  "e" for number of partitions in spark
  *  "file" text file having comma separated dimensions of each point
  *  for a particular c the minimum number of samples required will be n >= ((2+0.01)/(0.01)^2^)* ln(2/1-c)
  *  confidence deduced as per https://www.cs.princeto.edu/courses/archive/fall09/cos521/Handouts/probabilityandcomputing.pdf
  */
object CureAlgorithm {

  def start(cureArgs : CureArgs): RDD[String] = {

    val file = cureArgs.inputFile
    //validations of each attribute
    validateArgs(cureArgs, file)
    /**
      * start spark context now
      */

    val sparkContext = new SparkContext
    val sparkConf = sparkContext.getConf
    println(s"SPARK CONFIGS are ${sparkConf.getAll}")
    // Broadcast variables
    val broadcastK = sparkContext.broadcast(cureArgs.clusters)
    val broadcastM = sparkContext.broadcast(cureArgs.representatives)
    val broadcastRemoveOutliers = sparkContext.broadcast(cureArgs.removeOutliers)
    val sf = cureArgs.shrinkingFactor
    val broadcastSf = sparkContext.broadcast(sf)
    //load dataset
    val distFile = sparkContext.textFile(file)
    val size = distFile.count()
    val n = 5050 * Math.log(2/(1-cureArgs.confidence))// Number of random samples required
    val m = cureArgs.representatives
    println(s"Size of input file $file is $size and samples needed is $n")

    val tmp = Math.max(cureArgs.samplingRatio, 0.1)
    val samplingR = if (n/size > 1) 1 else Math.max(tmp,n/size )


    println(s"The sampling fraction is $samplingR")

    val topRecords = distFile.top(5)   // Sense the dimesions of points in dataset
    val dimensions = topRecords.foldLeft(Integer.MAX_VALUE){(min, curr) => {
      val size = curr.split(",").length
      if( size < min) size else min
    }}
    println(s"Sensed the records in data have $dimensions dimensions")
    val broadcastDimen = sparkContext.broadcast(dimensions)
    val sample = distFile.sample(withReplacement = false, fraction = samplingR).repartition(cureArgs.partitions)    // pick random samples as per sampling ratio and repartition to get k partitions
    println(s"The total size is $size and sampled count is ${sample.count()}")

    val points : RDD[Point] = sample.map(a => {  // assign cluster id to each point
      val p =Point(a.split(",").slice(0,broadcastDimen.value).map(_.toDouble))
      p.cluster = Cluster(Array(p),Array(p), null,p)
      p
    }).cache() // Cache the RDD of points such that the previous DAGS never run again

    // Start processing at each partition in executors
    val clusters = points.mapPartitions(partition => {
      val numRepresentatives = broadcastM.value
      val data = partition.toList
      val removeOutliers = broadcastRemoveOutliers.value
      val shrinkf = broadcastSf.value
      val numClusters = if(removeOutliers)broadcastK.value * 2 else broadcastK.value

      if(data.lengthCompare(numClusters) > 0) {
        val first = data.head
        val kdTree: KDTree = createKDTree(data, first)
        println("Created Kd Tree in partition")
        val cHeap: ClusterMinHeap = createHeap(data, kdTree)

        /**
          * While loop to get closest clusters and merge them.
          * Thus reducing number of clusters by 1 in each iteration
          */

        computeClustersAtPartitions(numClusters, numRepresentatives, shrinkf, kdTree, cHeap) // now we are complete with cluster merging at each executor. Will de-reference clusters and points
        if(removeOutliers) {
          0 until cHeap.heapSize foreach (i => if (cHeap.getDataArray(i).representatives.length < m) cHeap.remove(i))
          computeClustersAtPartitions(broadcastK.value, numRepresentatives, shrinkf, kdTree, cHeap)
        }
        cHeap.getDataArray.slice(0, cHeap.heapSize).map(cc => {
          cc.points.foreach(_.cluster = null)
          val reps = cc.representatives
          Cluster(findMFarthestPoints(cc.points, cc.mean , numRepresentatives), reps, null, cc.mean, cc.squaredDistance)
        }).toIterator
      }
      else {
        data.map(a => {
          Cluster(Array(a), Array(a), null, a)
        }).toIterator
      }}
    )
    //The spark driver now collects all the cluster. Remember that now every cluster has only m points.
    val cureClusters = clusters.collect()
    println(s"Partitioned Execution finished Sucessfully. Collected all ${cureClusters.length} clusters at driver")

    cureClusters.foreach(c => c.representatives.foreach(a =>{
      if(a!= null)a.cluster=c
    }))// Every representative point should reference its cluster id

    val reducedPoints = cureClusters.flatMap(_.representatives).toList
    val kdTree:KDTree = createKDTree(reducedPoints, reducedPoints.head)
    val cHeap: ClusterMinHeap = createHeapFromClusters(cureClusters.toList, kdTree)

    var clustersShortOfMReps = if(cureArgs.removeOutliers) cureClusters.count(_.representatives.length < m ) else 0
    while(cHeap.heapSize - clustersShortOfMReps > cureArgs.clusters) {
      val c1 = cHeap.takeHead()
      val nearest = c1.nearest
      val c2 = mergeClusterWithPointsAndRep(c1, nearest, cureArgs.representatives, sf) // merge the cluster and its nearest

      if(cureArgs.removeOutliers) {
        val a = nearest.representatives.length < m
        val b = c1.representatives.length < m
        val c = c2.representatives.length < m

        if(a && b && c) clustersShortOfMReps = clustersShortOfMReps -1
        else if (a && b) clustersShortOfMReps = clustersShortOfMReps -2
        else if(a||b) clustersShortOfMReps = clustersShortOfMReps -1
      }

      c1.representatives.foreach(kdTree.delete)              // delete cluster representatives from kdtree
      nearest.representatives.foreach(kdTree.delete)         // delete nearest cluster from kdtree
      val representArray = c2.representatives     // Use new representatives to get the nearest cluster
      val (newNearestCluster, nearestDistance) = getNearestCluster(representArray, kdTree)
      c2.nearest = newNearestCluster
      c2.squaredDistance = nearestDistance

      //println(s"current cluster rep :: ${c2.points.toList}  nearest cluster :: ${newNearestCluster.representatives.toList}")

      representArray.foreach(kdTree.insert) // Now add each point of representatives of the new cluster to kdTree
      removeClustersFromHeapUsingReps(kdTree, cHeap, c1, nearest)
      cHeap.insert(c2)
      println(s"Processing and merging clusters. Heap size is :: ${cHeap.heapSize}")
    }
    println(s"Merged clusters at driver. Total clusters ${cHeap.heapSize} Removed $clustersShortOfMReps clusters without $m repsenentatives")
    val finalClusters = cHeap.getDataArray.slice(0,cHeap.heapSize).filter(_.representatives.length >= m)
    finalClusters.zipWithIndex.foreach{case(x,i) => x.id = i } // assign cluster id
    println("Final Representatives")
    finalClusters.foreach(c => c.representatives.foreach(r => println(s"$r , ${c.id}")))
    val kdBroadcast = sparkContext.broadcast(kdTree)
    println("Broadcasting kdTree from driver to executors")

    distFile.mapPartitions(partn => {
      val kdTreeAtEx = kdBroadcast.value
      partn.map(p => {
        val readPoint = Point(p.split(',').slice(0,broadcastDimen.value).map(_.toDouble))
        p.concat(s",${kdTreeAtEx.closestPointOfOtherCluster(readPoint).cluster.id}")
      })
    })
  }

  //*****************************************************************************************************************************************************************
  //***************************************************************Main Method ends here ****************************************************************************
  //*****************************************************************************************************************************************************************
  private def computeClustersAtPartitions(numClusters: Int, numRepresentatives: Int, shrinkf: Double, kdTree: KDTree, cHeap: ClusterMinHeap):Unit = {
    var i =0
    while (cHeap.heapSize > numClusters) { //outliers will remain as single point clusters in most cases
      val c1 = cHeap.takeHead()
      val nearest = c1.nearest
      val c2 = mergeClusterWithPointsAndRep(c1, nearest, numRepresentatives, shrinkf) // merge the cluster and its nearest. When at executor level we do not need representatives shrinking
      c1.representatives.foreach(kdTree.delete)
      nearest.representatives.foreach(kdTree.delete)
      val (newNearestCluster, nearestDistance) = getNearestCluster(c2.representatives, kdTree) // Performance degrades as number of points in the cluster increases
      c2.nearest = newNearestCluster
      c2.squaredDistance = nearestDistance
      c2.representatives.foreach(kdTree.insert)
      removeClustersFromHeapUsingReps(kdTree, cHeap, c1, nearest)
      if (i % 256 == 0) println(s"Processing and merging clusters from heap. Current Total Cluster size is ${cHeap.heapSize}")
      i =i+1
      cHeap.insert(c2)
    }
  }

  private def validateArgs(args: CureArgs, file: String): Unit = {
    if (args.confidence - 1 >= 0) throw new Exception("Attribute confidence must be between and not including 0 and 1")
    if (args.shrinkingFactor - 0.99 >= 0) throw new Exception("Attribute shrinking factor must be between and not including 0 and 0.99")
    if (args.clusters < 0) throw new Exception("Please specify a positive integer value for the number of cluster")
    if (args.partitions < 0 || args.partitions > 100) throw new Exception("Please specify a positive integer value between 1 to 100 for the number of partitions")
    if (args.representatives <= 1) throw new Exception("Please specify a positive integer value >1 for the number of representatives in a clusters")
    println(s"Attributes for CURE Algorithm are:: Confidence:${args.confidence}  Number of clusters:${args.clusters}  Number of Representatives:${args.representatives}  Shrinking Factor:${args.shrinkingFactor}  Number of partitions:${args.partitions} Sampling:${args.samplingRatio}")
    println(s"Reading data for Cure Algo from path $file")
  }

  private def removeClustersFromHeapUsingReps(kdTree: KDTree, cHeap: ClusterMinHeap, c1: Cluster, nearest: Cluster): Unit = {
    val heapArray = cHeap.getDataArray // now we need to delete the cluster points of c1 and nearest from Heap
    val heapSize = cHeap.heapSize
    var it = 0
    while (it < heapSize) {
      var flag = false
      val tmpCluster = heapArray(it)
      val tmpNearest = tmpCluster.nearest
      if (tmpCluster == nearest){
        cHeap.remove(it) //remove cluster
        flag = true
      }
      if (tmpNearest == nearest || tmpNearest == c1) { //Re Compute nearest cluster
        val (newCluster, newDistance) = getNearestCluster(tmpCluster.representatives, kdTree)
        tmpCluster.nearest = newCluster
        tmpCluster.squaredDistance = newDistance
        cHeap.heapify(it)
        flag = true
      }
      if(!flag) it = it + 1
    }
  }

  private def createHeap(data: List[Point], kdTree: KDTree) = {
    val cHeap = ClusterMinHeap(data.length)
    data.map(p => {
      val closest = kdTree.closestPointOfOtherCluster(p)
      p.cluster.nearest = closest.cluster // Assign the closest cluster
      p.cluster.squaredDistance = p.squaredDistance(closest)
      cHeap.insert(p.cluster)
      p.cluster
    })
    cHeap
  }

  private def createHeapFromClusters(data: List[Cluster], kdTree: KDTree) = {
    val cHeap = ClusterMinHeap(data.length)
    data.foreach(p => {
      val (closest, distance) = getNearestCluster(p.representatives, kdTree)
      p.nearest = closest // Assign the closest cluster
      p.squaredDistance = distance
      cHeap.insert(p)
    })
    cHeap
  }

  private def createKDTree(data: List[Point], first: Point) : KDTree = {
    val kdTree = KDTree(KDNode(first, null, null), first.dimensions.length)
    for (i <- 1 until data.length - 1) {
      kdTree.insert(data(i))
    }
    kdTree
  }

  private def getNearestCluster(points: Array[Point], kdTree: KDTree): (Cluster, Double) = {
    val (point, distance) = points.foldLeft(points(0), Double.MaxValue) { // get the new nearest cluster. Start with a dummy value
      case ((nearestPoint, newD), rep) =>
        val closest = kdTree.closestPointOfOtherCluster(rep)
        val d = rep.squaredDistance(closest)
        if (d < newD) (closest, d)
        else (nearestPoint, newD)
    }
    (point.cluster, distance)
  }

  def copyPointsArray(oldArray: Array[Point]): Array[Point] = {
    val newArray = new Array[Point](oldArray.length)
    newArray.indices.foreach(i => {
      if(oldArray(i) == null)newArray(i) = null
      else newArray(i) = Point(oldArray(i).dimensions.clone())
    })
    newArray
  }

  def mergeClusterAndPoints(c1: Cluster, nearest: Cluster) : Cluster = {
    val mergedPoints = c1.points ++ nearest.points
    val mean = meanOfPoints(mergedPoints)
    val newCluster = Cluster(mergedPoints,null, null, mean)
    mergedPoints.foreach(_.cluster = newCluster)
    mean.cluster = newCluster
    newCluster
  }

  private def mergeClusterWithPointsAndRep(c1: Cluster, nearest: Cluster, repCount: Int, sf:Double) : Cluster = {

    val mergedPoints = c1.points ++ nearest.points
    val mean = meanOfPoints(mergedPoints)

    val mergedCl = {
      if (mergedPoints.length <= repCount) {
        Cluster(mergedPoints, shrinkRepresentativeArray(sf, mergedPoints, mean), null, mean)
      }
      else {
        val tmpArray = new Array[Point](repCount)
        for (i <- 0 until repCount) {          // Loop to compute the representatives. repCount number of representatives are required
          var maxDist = 0.0d
          var minDist = 0.0d
          var maxPoint: Point = null
          mergedPoints.foreach(p => {
            if(!tmpArray.contains(p)){
              if (i == 0) minDist = p.squaredDistance(mean)
              else {
                minDist = tmpArray.foldLeft(Double.MaxValue) { (maxd, r) => // select the point whose minimum distance is the most the current representatives already computed.
                {
                  if(r == null) maxd
                  else {
                    val dist = p.squaredDistance(r)
                    if (dist < maxd) dist
                    else maxd
                  }
                }
                }
              }
              if (minDist >= maxDist) {
                maxDist = minDist
                maxPoint = p
              }
            }})
          tmpArray(i) = maxPoint
        }

        val representatives = shrinkRepresentativeArray(sf, tmpArray, mean)
        val newCluster = Cluster(mergedPoints,representatives, null, mean)
        newCluster
      }
    }
    mergedCl.representatives.foreach(_.cluster = mergedCl)
    mergedCl.points.foreach(_.cluster = mergedCl)
    mergedCl.mean.cluster = mergedCl
    mergedCl
  }

  private def findMFarthestPoints(points: Array[Point], mean: Point, m: Int): Array[Point] = {
    val tmpArray = new Array[Point](m)
    for (i <- 0 until m) {          // Loop to compute the representatives. repCount number of representatives are required
      var maxDist = 0.0d
      var minDist = 0.0d
      var maxPoint: Point = null
      points.foreach(p => {
        if(!tmpArray.contains(p)){
          if (i == 0) minDist = p.squaredDistance(mean)
          else {
            minDist = tmpArray.foldLeft(Double.MaxValue) { (maxd, r) => { // optimization possible by storing computed distance
              if(r == null) maxd
              else {
                val dist = p.squaredDistance(r)
                if (dist < maxd) dist
                else maxd
              }
            }
            }
          }
          if (minDist >= maxDist) {
            maxDist = minDist
            maxPoint = p
          }
        }})
      tmpArray(i) = maxPoint
    }
    tmpArray.filter(_!=null)
  }


  private def shrinkRepresentativeArray(sf: Double, tmpArray: Array[Point], mean:Point) = {
    val repArray = copyPointsArray(tmpArray)
    repArray.foreach(rep => { // Shrink representative array by shrinking factor
      if(rep!=null) {
        val repDim = rep.dimensions
        repDim.indices.foreach(i => repDim(i) = repDim(i) + (mean.dimensions(i) - repDim(i)) * sf)
      }
    })
    repArray
  }

  def meanOfPoints(points: Array[Point]): Point = {

    val len = points(0).dimensions.length
    val newArray = new Array[Double](len)
    points.filter(_!=null).foreach(p => {
      val d = p.dimensions
      d.indices.foreach(i => newArray(i)+=d(i))
    })
    newArray.indices.foreach(j => newArray(j) = newArray(j)/points.length)
    Point(newArray)
  }
}
