package com.clustering.kdtree

import com.clustering.point.Point

/**
  * Tightly coupled with cluster implementation.
  * Every point is expected to belong to a cluster
  *
  * Geometric data structure to store points in a tree structure
  *
  * @param root the starting node
  * @param k  dimensions length of points of node
  */
case class KDTree(var root: KDNode, k:Int) {

  def newNode(point: Point) = KDNode(point, null, null)

  /**
    * Insert a new node in KD Tree
    */
  def insert(point : Point): KDNode = insertRec(this.root, point, 0)

  /**
    * Recursive Insert. Recursively travel the depth of kd tree and then insert.
    */
  def insertRec(node: KDNode, point: Point, depth: Int): KDNode = {
    if (node == null) newNode(point:Point)
    else {
      val axis = depth % k
      if (point.dimensions(axis) < node.point.dimensions(axis)) node.left = insertRec(node.left, point, depth + 1)
      else if(matchPoints(node.point, point)) {
        node.point.cluster = point.cluster
        node.deleted = false
      }
      else node.right = insertRec(node.right, point, depth + 1)
      node
    }
  }

  def matchPoints(point1: Point, point2: Point): Boolean = {
    val p1 = point1.dimensions
    val p2 = point2.dimensions
    !p1.indices.exists(i => p1(i) != p2(i))
  }

  def search(point:Point) : Boolean = searchRec(this.root, point, 0)

  def searchRec(node: KDNode, point: Point, depth: Int): Boolean = {
    if(node == null) false
    else{
      if (matchPoints(node.point, point)){
        if(!node.deleted) true
        else false
      }
      else{
        val axis =  depth % k
        if(point.dimensions(axis) < node.point.dimensions(axis) )
          searchRec(node.left, point, depth+1)
        else
          searchRec(node.right, point, depth+1)
      }
    }
  }

  //Delete point from KD Tree
  def delete(point:Point) : KDNode = {
    deleteRec(this.root, point, 0)
  }

  def deleteRec(node: KDNode, point: Point, depth: Int) : KDNode = {
    if(node != null) {
      val axis = depth % k
      if (matchPoints(node.point, point)) {
        node.deleted = true
      }
      else {
        if (point.dimensions(axis) < node.point.dimensions(axis)) node.left = deleteRec(node.left, point, depth + 1)
        else node.right = deleteRec(node.right, point, depth + 1)
      }
    }
    node
  }

  def copyPoint(p1: Point, p2: Point): Unit = {
    val d1 = p1.dimensions
    val d2 = p2.dimensions
    d1.indices.foreach(i => d1(i) = d2(i))
  }

  // Recursively finds minimum of d'th dimension in KD tree
  // The parameter depth is used to determine current axis.
  def findMinRec(root:KDNode, d:Int, depth: Int) : KDNode =
  {
    // Base cases
    if (root == null)null
    else {
      // Current dimension is computed using current depth and total
      // dimensions (k)
      val axis = depth % k

      // Compare point with root with respect to cd (Current dimension)
      if (axis == d) {
        if (root.left == null) root
        else findMinRec(root.left, d, depth + 1)
      }
      else {
        // If current dimension is different then minimum can be anywhere
        // in this subtree
        minNode(root, findMinRec(root.left, d, depth + 1), findMinRec(root.right, d, depth + 1), d)
      }
    }
  }

  // A wrapper over findMinRec(). Returns minimum of d'th dimension
  def findMin(node:KDNode, d: Int) : KDNode = findMinRec(node, d, 0)

  // A utility function to find minimum of three integers
  def minNode(x:KDNode, y: KDNode, z: KDNode, d : Int) : KDNode = {
    val res = {
      if (y != null && y.point.dimensions(d) < x.point.dimensions(d)) y
      else x
    }
    if (z != null && z.point.dimensions(d) < res.point.dimensions(d)) z
    else res
  }

  def closestPointOfOtherCluster(point:Point) : Point = {
    val c = closestRec(this.root, point, 0)
    if(c == null) null
    else c.point
  }

  def closestRec(node: KDNode, point: Point, depth: Int):KDNode = if (node == null) null
  else {
    val axis = depth % k
    if (point.cluster == node.point.cluster) { // cannot have points of the same cluster as nearest neighbour
      closerDistance(point, closestRec(node.left, point, depth + 1), closestRec(node.right, point, depth + 1)) // Assume left to find the nearest neighbour
    }
    else if (point.dimensions(axis) < node.point.dimensions(axis)) {
      val best = {
        if (node.deleted)
          closestRec(node.left, point, depth + 1)
        else
          closerDistance(point, closestRec(node.left, point, depth + 1), node)
      }
      if (best == null) closerDistance(point, closestRec(node.right, point, depth + 1), best)
      else if (point.squaredDistance(best.point) > Math.abs(point.dimensions(axis)) - node.point.dimensions(axis))
        closerDistance(point, closestRec(node.right, point, depth + 1), best)
      else best
    }
    else {
      val best = {
        if (node.deleted) closestRec(node.right, point, depth + 1)
        else closerDistance(point, closestRec(node.right, point, depth + 1), node)
      }
      if(best == null) closerDistance(point, closestRec(node.left, point, depth + 1), best)
      else if (point.squaredDistance(best.point) > Math.pow(point.dimensions(axis) - node.point.dimensions(axis), 2))
        closerDistance(point, closestRec(node.left, point, depth + 1), best)
      else best
    }
  }

  def closerDistance(pivot: Point, n1:KDNode, n2:KDNode) :KDNode = {
    if (n1 == null) n2
    else if (n2 == null) n1
    else {
      val d1 = pivot.squaredDistance(n1.point)
      val d2 = pivot.squaredDistance(n2.point)
      if (d1 < d2) n1
      else n2
    }
  }
}

case class KDNode(point:Point, var left:KDNode, var right:KDNode, var deleted:Boolean = false)
