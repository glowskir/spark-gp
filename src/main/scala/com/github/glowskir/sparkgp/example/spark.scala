package com.github.glowskir.sparkgp.example


import com.github.glowskir.sparkgp.SparkMigrationEA
import fuel.func.RunExperiment
import fuel.moves.BitSetMoves
import fuel.util._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.BitSet

object BroadcastTest {
  def main(args: Array[String]) {

    val bcName = if (args.length > 2) args(2) else "Http"
    val blockSize = if (args.length > 3) args(3) else "4096"

    val sparkConf = new SparkConf().setAppName("Broadcast Test")
      .set("spark.broadcast.factory", s"org.apache.spark.broadcast.${bcName}BroadcastFactory")
      .set("spark.broadcast.blockSize", blockSize)
    val sc = new SparkContext(sparkConf)

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime
      val barr1 = sc.broadcast(arr1)
      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr1.value.size)
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }

    sc.stop()
  }
}
object MaxOnesTest {
  def main(args: Array[String]) {
    implicit lazy val opt = Options(args)
    implicit lazy val rng = Rng(opt)
    implicit lazy val coll = new CollectorFile(opt)
    val sparkConf = new SparkConf().setAppName("MaxOnesTest Test")
    implicit val sc = new SparkContext(sparkConf)
    RunExperiment(new SparkMigrationEA(
      BitSetMoves(100),
      (s: BitSet) => s.size,
      (s: BitSet, e: Int) => e == 0
    ))

    sc.stop()
  }
}