package com.clustering.point

import org.specs2.mutable._


class TestPoint extends Specification {
  "Test Distance between two points" should {

    "distance should be 5 units" in {
      val p1 = Point(Array(-2,3,6))
      val p2 = Point(Array(2,3,9))

      p1.distance(p2) must_=== 5.0d
    }

    "distance should be 15.55 units" in {
      val p1 = Point(Array(-2,2,6))
      val p2 = Point(Array(2,3,-9))

      p1.distance(p2) must_=== 15.556349186104045d
    }

    "distance should be 0 units" in {
      val p1 = Point(Array(2,3,4))
      val p2 = Point(Array(2,3,4))

      p1.distance(p2) must_=== 0.0d
    }

    "distance function throws Exceptio due to dimension length mismatch between two points" in {
      val p1 = Point(Array(2,3,4))
      val p2 = Point(Array(2,3,4,5))

      p1.distance(p2) must_==0
    }

    "distance function throws Exceptio due to null point" in {
      val p1 = Point(Array(2,3,4))
      val p2 = null

      p1.distance(p2) must throwA [Exception]
    }
  }
}
