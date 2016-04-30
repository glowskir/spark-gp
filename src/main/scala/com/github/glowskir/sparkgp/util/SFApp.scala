package com.github.glowskir.sparkgp.util

import fuel.util.FApp
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by glowskir on 25.04.16.
  */
trait SApp extends Logging {
  implicit lazy val sc = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val newsc = new SparkContext(conf)
    log.warn(conf.toDebugString)
    newsc.setLogLevel("WARN")
    newsc
  }
}
