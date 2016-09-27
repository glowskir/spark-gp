package com.github.glowskir.sparkgp

import org.apache.spark.rdd.RDD

/**
  * Created by glowskir on 05.04.16.
  */
package object core {
  type SparkStatePop[T] = Seq[RDD[T]]
  implicit class SparkStatePopOps[T] (val internal:SparkStatePop[T]) {
  }
}
