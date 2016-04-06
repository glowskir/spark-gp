package com.github.glowskir.sparkgp.moves

import fuel.func.SearchOperator
import org.apache.spark.rdd.RDD

/**
  * Created by glowskir on 06.04.16.
  */
trait SparkMoves[S] extends Serializable{
  def newSolution: S

  def moves: RDD[SearchOperator[S]]
}
