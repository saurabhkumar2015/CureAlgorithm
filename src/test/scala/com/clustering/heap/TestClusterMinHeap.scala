package com.clustering.heap

import com.clustering.cure.Cluster
import org.specs2.mutable.Specification

class TestClusterMinHeap extends Specification {

  "Test Remove of cluster min heap" should {
    "insert clusters and remove them" in {

      val cHeap = ClusterMinHeap(20)
      cHeap.insert(Cluster(null, null, null, null, 12))
      cHeap.insert(Cluster(null, null, null, null, 23))
      cHeap.insert(Cluster(null, null, null, null, 7))
      cHeap.insert(Cluster(null, null, null, null, 2))
      cHeap.insert(Cluster(null, null, null, null, 19))
      cHeap.insert(Cluster(null, null, null, null, 44))
      cHeap.insert(Cluster(null, null, null, null, 74))
      cHeap.insert(Cluster(null, null, null, null, 29))
      cHeap.insert(Cluster(null, null, null, null, 1.1d))
      cHeap.insert(Cluster(null, null, null, null, 11))
      cHeap.insert(Cluster(null, null, null, null, 5))

      cHeap.takeHead().squaredDistance must_=== 1.1d
      cHeap.takeHead().squaredDistance must_=== 2d
      cHeap.takeHead().squaredDistance must_=== 5d
      cHeap.takeHead().squaredDistance must_=== 7d
      cHeap.takeHead().squaredDistance must_=== 11d
      cHeap.takeHead().squaredDistance must_=== 12d
      cHeap.takeHead().squaredDistance must_=== 19d
      cHeap.takeHead().squaredDistance must_=== 23d
      cHeap.takeHead().squaredDistance must_=== 29d
      cHeap.takeHead().squaredDistance must_=== 44d
      cHeap.takeHead().squaredDistance must_=== 74d

      cHeap.getDataArray(0) must_=== null

    }

    "insert clusters and deletes some of them" in {

      val cHeap = ClusterMinHeap(20)
      cHeap.insert(Cluster(null, null, null, null, 12))
      cHeap.insert(Cluster(null, null, null, null, 23))
      cHeap.insert(Cluster(null, null, null, null, 7))
      cHeap.insert(Cluster(null, null, null, null, 2))
      cHeap.insert(Cluster(null, null, null, null, 19))
      cHeap.insert(Cluster(null, null, null, null, 44))
      cHeap.insert(Cluster(null, null, null, null, 74))
      cHeap.insert(Cluster(null, null, null, null, 29))
      cHeap.insert(Cluster(null, null, null, null, 1.1d))
      cHeap.insert(Cluster(null, null, null, null, 11))
      cHeap.insert(Cluster(null, null, null, null, 5))

      cHeap.remove(cHeap.heapSize)
      cHeap.remove(0)

      cHeap.takeHead().squaredDistance must_=== 2d
      cHeap.takeHead().squaredDistance must_=== 5d
      cHeap.takeHead().squaredDistance must_=== 7d
      cHeap.takeHead().squaredDistance must_=== 11d
      cHeap.takeHead().squaredDistance must_=== 12d
      cHeap.takeHead().squaredDistance must_=== 19d
      cHeap.takeHead().squaredDistance must_=== 23d
      cHeap.takeHead().squaredDistance must_=== 29d
      cHeap.takeHead().squaredDistance must_=== 74d

      cHeap.getDataArray(0) must_=== null

    }

    "insert clusters and update some of them" in {

      val cHeap = ClusterMinHeap(20)
      cHeap.insert(Cluster(null, null, null, null, 12))
      cHeap.insert(Cluster(null, null, null, null, 23))
      cHeap.insert(Cluster(null, null, null, null, 7))
      cHeap.insert(Cluster(null, null, null, null, 2))
      cHeap.insert(Cluster(null, null, null, null, 19))
      cHeap.insert(Cluster(null, null, null, null, 44))
      cHeap.insert(Cluster(null, null, null, null, 74))
      cHeap.insert(Cluster(null, null, null, null, 29))
      cHeap.insert(Cluster(null, null, null, null, 1.1d))
      cHeap.insert(Cluster(null, null, null, null, 11))
      cHeap.insert(Cluster(null, null, null, null, 5))

      cHeap.update(0,Cluster(null, null, null, null, 100))
      cHeap.update(0, Cluster(null, null, null, null, -1))
      cHeap.update(cHeap.heapSize-1, Cluster(null, null, null, null, -2))

      cHeap.takeHead().squaredDistance must_=== -2d
      cHeap.takeHead().squaredDistance must_=== -1d
      cHeap.takeHead().squaredDistance must_=== 5d
      cHeap.takeHead().squaredDistance must_=== 7d
      cHeap.takeHead().squaredDistance must_=== 11d
      cHeap.takeHead().squaredDistance must_=== 12d
      cHeap.takeHead().squaredDistance must_=== 23d
      cHeap.takeHead().squaredDistance must_=== 29d
      cHeap.takeHead().squaredDistance must_=== 44d
      cHeap.takeHead().squaredDistance must_=== 74d
      cHeap.takeHead().squaredDistance must_=== 100d

      cHeap.getDataArray(0) must_=== null

    }
  }

}
