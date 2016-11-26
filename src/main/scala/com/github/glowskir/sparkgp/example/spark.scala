package com.github.glowskir.sparkgp.example


import com.github.glowskir.sparkgp.SparkMigrationEA
import fuel.func.RunExperiment
import fuel.moves.{BitSetMoves, PermutationMoves}
import fuel.util._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.BitSet


object MaxOnesTest {
  def main(args: Array[String]) {
    implicit lazy val opt = Options(args)
    implicit lazy val rng = Rng(opt)
    implicit lazy val coll = new CollectorFile(opt)
    val maxOnesSize = opt('maxOnesSize, 10000, (_: Int) >= 0)
    val sparkConf = new SparkConf().setAppName("MaxOnesTest")
    implicit val sc = new SparkContext(sparkConf)
    RunExperiment(new SparkMigrationEA(
      BitSetMoves(maxOnesSize),
      (s: BitSet) => s.size,
      (s: BitSet, e: Int) => e == 0
    ))
    sc.stop()
  }
}

object TSPTest {
  def main(args: Array[String]) {
    implicit lazy val opt = Options('numCities -> 30, 'maxGenerations -> 300) ++ Options(args)
    implicit lazy val rng = Rng(opt)
    implicit lazy val coll = new CollectorFile(opt)
    val sparkConf = new SparkConf().setAppName("TSPTest")
    implicit val sc = new SparkContext(sparkConf)

    val numCities = opt('numCities, (_: Int) > 0)
    val cities = Seq.fill(numCities)((rng.nextDouble, rng.nextDouble))
    val distances = for (i <- cities) yield for (j <- cities)
      yield math.hypot(i._1 - j._1, i._2 - j._2)

    def eval(s: Seq[Int]) =
      s.indices.map(i => distances(s(i))(s((i + 1) % s.size))).sum

    RunExperiment(new SparkMigrationEA(PermutationMoves(numCities), eval))
    sc.stop()
  }
}