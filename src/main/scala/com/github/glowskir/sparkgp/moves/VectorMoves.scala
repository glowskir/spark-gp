package com.github.glowskir.sparkgp.moves

import fuel.func.SearchOperator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by glowskir on 06.04.16.
  */
trait VectorMoves[S] extends SparkMoves[S] {
  def context: SparkContext
  def onePointMutation: (S) => S
  def onePointCrossover: (S, S) => (S, S)
  def twoPointCrossover: (S, S) => (S, S)

  override def moves: RDD[SearchOperator[S]] = {
    context.makeRDD(Seq(
      SearchOperator(onePointMutation),
      SearchOperator(onePointCrossover),
      SearchOperator(twoPointCrossover)
    ))
  }
}
