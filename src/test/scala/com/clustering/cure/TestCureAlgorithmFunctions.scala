package com.clustering.cure

import com.clustering.point.Point
import org.specs2.mutable.Specification

class TestCureAlgorithmFunctions extends Specification{

  "MergeClusterAndPoints functionality test" should {
    "Two cluster merge" in {
      val p1 = Point(Array(2,3,4))
      val p2 = Point(Array(2,3,5))
      val p3 = Point(Array(3,3,5))
      val p4 = Point(Array(8,3,5))
      val p5 = Point(Array(7,3,5))
      val p6 = Point(Array(6,3,6))

      val c1 = Cluster(Array(p1,p2,p3),Array(p1,p2,p3),null,null)
      val c2 = Cluster(Array(p4,p5,p6),Array(p4,p5,p6),null,null,2.3d)

      val c3 = CureAlgorithm.mergeClusterAndPoints(c2, c1)

      c3.points.toSeq must contain (p1,p2,p3,p4,p5,p6)
    }
  }
}
