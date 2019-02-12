package com.clustering.cure

import com.clustering.point.Point

case class Cluster(points : Array[Point],
                   var representatives : Array[Point],
                   var nearest : Cluster,
                   var mean: Point,
                   var squaredDistance : Double =0.0d,
                   var id:Int =0)
