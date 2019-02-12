package com.clustering.kdtree

import com.clustering.point.Point
import org.specs2.mutable.Specification

class TestKDTree extends Specification {

  "Test Insert of points in kd tree" should {
    "insert point (2,3,4) and (7,8,9),(7,8,10),(7,9,9) and then delete them" in {
      val kdNode = KDNode(Point(Array(2,3,4), null),null,null)
      val point1 = Point(Array(7,8,9))
      val point2 = Point(Array(7,8,10))
      val point3 = Point(Array(7,9,9))
      val point4 = Point(Array(7.5,9,9))
      val point5 = Point(Array(7,9,9.5))
      val point6 = Point(Array(7,9.5,9))
      val point7 = Point(Array(7.9,9,12))
      val point8 = Point(Array(-7.9,9,12))
      val point9 = Point(Array(7.9,-9,12))
      val point10 = Point(Array(7.9,9,-12))
      val point11 = Point(Array(7.9,9,-13))
      val point12 = Point(Array(7.9,-9.5,12))



      val kdTree = KDTree(root = kdNode , k = 3)

      //Test Insert in KD Tree
      val l1 = List(point1,point2,point3,point4,point5,point6,point7,point8
        ,point9, point10, point11,point12)
        l1.foreach(kdTree.insert)

      kdTree.search(Point(Array(7,8,9))) must_===true
      kdTree.search(Point(Array(7,8,10))) must_===true
      kdTree.search(Point(Array(2,3,4))) must_===true
      kdTree.search(Point(Array(7,9,9))) must_===true
      kdTree.search(Point(Array(2,4,4))) must_===false
      kdTree.search(Point(Array(7,8,8))) must_===false

      //Test delete in KD Tree
      kdTree.delete(Point(Array(7,8,10)))
      kdTree.search(Point(Array(7,8,10))) must_===false

      kdTree.delete(Point(Array(2,3,4)))
      kdTree.search(Point(Array(7,8,10))) must_===false
      kdTree.search(Point(Array(2,3,4))) must_===false
      kdTree.search(Point(Array(7,8,9))) must_===true
      kdTree.search(Point(Array(7,9,9))) must_===true

      kdTree.delete(Point(Array(7,8,9)))
      kdTree.search(Point(Array(7,8,10))) must_===false
      kdTree.search(Point(Array(7,8,9))) must_===false
      kdTree.search(Point(Array(2,3,4))) must_===false
      kdTree.search(Point(Array(7,9,9))) must_===true

      // Test another point deleted
      kdTree.delete(Point(Array(7,9,9)))
      kdTree.search(Point(Array(7,8,10))) must_===false
      kdTree.search(Point(Array(7,8,9))) must_===false
      kdTree.search(Point(Array(2,3,4))) must_===false
      kdTree.search(Point(Array(7,9,9))) must_===false

      // delete all remaining points
      List(point4,point5,point6,point7,point8,point9,point10,point11,point12).foreach(kdTree.delete)
      kdTree.search(point1) must_===false
      kdTree.search(point2) must_===false
      kdTree.search(point3) must_===false
      kdTree.search(point4) must_===false
      kdTree.search(point5) must_===false
      kdTree.search(point6) must_===false
      kdTree.search(point7) must_===false
      kdTree.search(point8) must_===false
      kdTree.search(point9) must_===false
      kdTree.search(point10) must_===false
      kdTree.search(point11) must_===false
      kdTree.search(point12) must_===false

      //Insert and search again
      l1.foreach(kdTree.insert)
      kdTree.search(point1) must_===true
      kdTree.search(point2) must_===true
      kdTree.search(point3) must_===true
      kdTree.search(point4) must_===true
      kdTree.search(point5) must_===true
      kdTree.search(point6) must_===true
      kdTree.search(point7) must_===true
      kdTree.search(point8) must_===true
      kdTree.search(point9) must_===true
      kdTree.search(point10) must_===true
      kdTree.search(point11) must_===true
      kdTree.search(point12) must_===true

    }
  }
}
