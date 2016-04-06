package com.github.glowskir.sparkgp

import fuel.util._
import org.apache.spark.rdd.RDD

/**
  * Created by glowskir on 04.04.16.
  */

trait SparkSelection[S, E] extends (RDD[(S, E)] => (S, E))

class SparkTournamentSelection[S, E](ordering: Ordering[E], val tournamentSize: Int)
  extends SparkSelection[S, E] {
  def this(o: Ordering[E])(implicit opt: Options) =
    this(o, opt('tournamentSize, 7, (_: Int) >= 2))

  def apply(pop: RDD[(S, E)]) = {
    pop.takeSample(true, tournamentSize).minBy(_._2)(ordering)
  }
}