package com.clustering.point

import com.clustering.cure.Cluster

case class Point (dimensions:Array[Double],
                  var cluster:Cluster = null) {
  /**
    * Returns the actual distance
    * @return Double
    */
  def  distance(p: Point) : Double= {
    val  d1 = this.dimensions
    val  d2 = p.dimensions
    Math.sqrt(d1.indices.foldLeft(0.0d) { (l, r) => l + Math.pow( d1(r) - d2(r),2)})
  }

  /**
    * Most used distance caluclator. It saves mathematical compute by not calling square root
    * @return Double
    */
  def  squaredDistance(p: Point) : Double= {
    val  d1 = this.dimensions
    val  d2 = p.dimensions
    d1.indices.foldLeft(0.0d) { (l, r) => l + Math.pow(d1(r) - d2(r), 2) }
  }

  override def equals(obj: scala.Any): Boolean = {
    !obj.asInstanceOf[Point].dimensions.indices.exists(i => this.dimensions(i) != obj.asInstanceOf[Point].dimensions(i))
  }

  override def toString: String = dimensions.toList.toString()
}
