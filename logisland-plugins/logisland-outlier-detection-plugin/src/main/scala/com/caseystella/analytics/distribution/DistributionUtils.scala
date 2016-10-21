/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
