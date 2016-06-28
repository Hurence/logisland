package com.caseystella.analytics.distribution

import com.twitter.algebird.QTree
import com.twitter.algebird.QTreeSemigroup
import scala.collection.JavaConversions._

object DistributionUtils {
  def createTree(values: java.lang.Iterable[java.lang.Double] ) : QTree[Double] = {
    val semigroup = new QTreeSemigroup[Double](6);
    iterableAsScalaIterable(values).map{QTree(_)}.reduce(merge(_, _, semigroup))
  }
  def createTree(value:java.lang.Double) : QTree[Double] = {
    QTree(value)
  }

  def merge(left:QTree[Double], right:QTree[Double], semigroup :QTreeSemigroup[Double] = new QTreeSemigroup(6)) : QTree[Double] = {
    semigroup.plus(left, right)
  }
  def merge(left:QTree[Double], right:QTree[Double]) : QTree[Double] = {
    merge(left, right, new QTreeSemigroup(6))
  }
}
