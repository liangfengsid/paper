/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import breeze.linalg.{Vector, DenseVector, squaredDistance}
import org.apache.spark.{Partitioner,HashPartitioner}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * K-means clustering.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.mllib.clustering.KMeans
 */
object KMeans {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of KMeans Clustering and is given as an example!
        |Please use the KMeans method found in org.apache.spark.mllib.clustering
        |for more conventional use.
      """.stripMargin)
  }

  class KMeansPartitioner(partitions: Int, points: Array[Vector[Double]]) extends Partitioner {
    def numPartitions: Int = partitions
    def centers: Array[Vector[Double]]= points

    def nonNegativeMod(x: Int, mod: Int): Int = {
      val rawMod = x % mod
      rawMod + (if (rawMod < 0) mod else 0)
    }

    def getPartition(key: Any): Int = key match {
      case null => 0
      case point:Vector[Double] => {
        val index = closestPoint(point, centers)
        nonNegativeMod(index, numPartitions)
      }
      case _ => 0
    }

    override def equals(other: Any): Boolean = other match {
      case h: KMeansPartitioner =>{
        h.numPartitions == numPartitions
        h.centers == centers
      }
      case _ =>
        false
    }

    override def hashCode: Int = numPartitions
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: KMeans <file> <k> <convergeDist> [<maxIteration>] [<isPartitioned>] [<partitionIter>] [<numPartitions>]")
      System.exit(1)
    }

    showWarning()

    val sparkConf = new SparkConf().setAppName("SparkKMeans")
    val sc = new SparkContext(sparkConf)

    //val args=Array("/data/page/pagecounts-20150101-010000","20","1.2","false","1","16")
    
    val K = args(1).toInt
    val convergeDist = args(2).toDouble
    val maxIter=if(args.length >3) args(3).toInt else -1
    val isPartitioned= if(args.length >4) args(4).toBoolean else false
    val partIter=if (args.length>5) args(5).toInt else 1
    val numPartitions=if (args.length>6) args(6).toInt else 16

    val lines = sc.textFile(args(0),numPartitions)
    val data = lines.map(line => {
      val s=line.split(" ")
      parseVector(s(2)+" "+s(3))
      }).cache()

    val kPoints = data.takeSample(withReplacement = false, K, 42).toArray
    //var tempDist = 1.0
    var tempDist= Double.PositiveInfinity

    var i=0

    var dataPair=data.map(p=>(p,1))

    while(tempDist > convergeDist && ((maxIter>=0 && i<maxIter) || maxIter<0)) {
      if(i==partIter){
        if(isPartitioned){
          val partitioner= new KMeansPartitioner(numPartitions,kPoints)
          dataPair=dataPair.reduceByKey(partitioner, (a,b)=>1).cache
          //data.unpersist(false)
        }else{
          val partitioner= new HashPartitioner(numPartitions)
          dataPair=dataPair.reduceByKey(partitioner, (a,b)=>1).cache
          //data.unpersist(false)
        }
      }
      
      val closest = if(i>=partIter){
        dataPair.map(p => (closestPoint(p._1, kPoints), (p._1, 1)) )
      }else{
        data.map (p=>(closestPoint(p, kPoints), (p, 1)) )
      }

      val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}
      //val pointStats = closest.reduceByKey( (x, y) => (x._1+y._1, x._2 + y._2), numPartitions)

      val newPoints = pointStats.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finished iteration "+ i + " (delta = " + tempDist + ")")
      i=i+1
    }

    println("Final centers:")
    kPoints.foreach(println)
    sc.stop()
  }
}
