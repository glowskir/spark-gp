package com.github.glowskir.sparkgp.func

import fuel.func.SearchOperator
import fuel.util.Options
import org.apache.spark.rdd.RDD

///**
//  * TODO: handle probability options
//  *
//  */
object SparkRandomMultiOperator {
  def apply[S](pipes: RDD[SearchOperator[S]])(implicit opt: Options) = {
    () => pipes.takeSample(true, 1).head
  }
}