package com.github.glowskir.sparkgp

import org.apache.spark.sql.{Dataset, Encoder, SQLContext}

import scala.reflect.ClassTag

/**
  * Created by glowskir on 05.04.16.
  */
package object core {
  type SparkStatePop[T] = RDD[T]

}
