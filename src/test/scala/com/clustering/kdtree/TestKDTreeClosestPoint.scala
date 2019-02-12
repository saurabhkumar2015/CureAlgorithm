package com.clustering.kdtree

import com.clustering.cure.Cluster
import com.clustering.point.Point
import org.specs2.mutable.Specification

class TestKDTreeClosestPoint extends Specification {

  "Test Closest point in kd tree" should {
    "insert point (2,3,4) and (7,8,9),(7,8,10),(7,9,9) etc and then check closest neightbour And nearest cluster algo" in {
      val c0 = Cluster(null,null,null,null, 0d)
      val c1 = Cluster(null,null,null,null, 1d)
      val c2 = Cluster(null,null,null,null, 2d)
      val c3 = Cluster(null,null,null,null, 3d)
      val c4 = Cluster(null,null,null,null, 4d)
      val c5 = Cluster(null,null,null,null, 5d)
      val c6 = Cluster(null,null,null,null, 6d)
      val c7 = Cluster(null,null,null,null, 7d)
      val c8 = Cluster(null,null,null,null, 8d)
      val c9 = Cluster(null,null,null,null, 9d)
      val c10 = Cluster(null,null,null,null, 10d)
      val c11 = Cluster(null,null,null,null, 11d)
      val c12 = Cluster(null,null,null,null, 12d)

      val point =Point(Array(2,3,4), cluster = c0)
      val kdNode = KDNode(point,null,null)
      val point1 = Point(Array(7,8,9), cluster = c1)
      val point2 = Point(Array(7,8,10), cluster = c2)
      val point3 = Point(Array(7,9,9), cluster = c3)
      val point4 = Point(Array(8,9,12), cluster = c4)
      val point5 = Point(Array(-8,-9,-12), cluster = c5)
      val point6 = Point(Array(-1,-1,-1), cluster = c6)
      val point7 = Point(Array(-1,-1,-2), cluster = c7)
      val point8 = Point(Array(0,-1,-2), cluster = c8)
      val point9 = Point(Array(-2,-1,-2), cluster = c9)
      val point10 = Point(Array(0,0,0), cluster = c10)
      val point11 = Point(Array(-0.1,0,0), cluster = c11)
      val point12 = Point(Array(-0.2,0,0.1), cluster = c12)
      val point13 = Point(Array(-0.21,0,0.1), cluster = c12)
      val point14 = Point(Array(-0.2,0.01,0.1), cluster = c12)
      val point15 = Point(Array(-0.2,0.01,0.4), cluster = c12)
      val point16 = Point(Array(-0.2,0.01,0.56), cluster = c12)

      val kdTree = KDTree(root = kdNode , k = 3)

      //Test Insert in KD Tree
      List(point2,point1,point4,point3,point5,point6,
        point7,point8,point9,point10,point11,point11,
        point12,point13,point14,point15,point16).foreach(kdTree.insert)

      //Test All closest points
      kdTree.closestPointOfOtherCluster(point3).dimensions mustEqual Array(7,8,9)
      kdTree.closestPointOfOtherCluster(point).dimensions mustEqual Array(-0.2,0.01,0.56)
      kdTree.closestPointOfOtherCluster(point12).dimensions mustEqual Array(-0.1,0,0)
      kdTree.closestPointOfOtherCluster(point10).dimensions mustEqual Array(-0.1,0,0)
      kdTree.closestPointOfOtherCluster(Point(Array(6,8,9.4),cluster = c1)).dimensions mustEqual Array(7,8,10)
      kdTree.closestPointOfOtherCluster(Point(Array(7,8,9.6), cluster = c2)).dimensions mustEqual Array(7,8,9)
      kdTree.closestPointOfOtherCluster(Point(Array(9,12,12), cluster = c4)).dimensions mustEqual Array(7,9,9)
      kdTree.closestPointOfOtherCluster(Point(Array(-2,-1,-1), cluster = c0)).dimensions mustEqual Array(-1,-1,-1)
      kdTree.closestPointOfOtherCluster(Point(Array(-2,-1,-1.1), cluster = c0)).dimensions mustEqual Array(-2,-1,-2)
      kdTree.closestPointOfOtherCluster(Point(Array(-0.2,0.01,0.55), cluster = c12)).dimensions mustEqual Array(-0.1,0,0)
      kdTree.closestPointOfOtherCluster(Point(Array(-0.2,0.01,0.55), cluster = c11)).dimensions mustEqual Array(-0.2,0.01,0.56)

      //delete and test
      kdTree.delete(Point(Array(-2,-1,-2)))
      kdTree.closestPointOfOtherCluster(Point(Array(-2,-1,-1.1), cluster = c0)).dimensions mustEqual Array(-1,-1,-1)


    }
  }
}
