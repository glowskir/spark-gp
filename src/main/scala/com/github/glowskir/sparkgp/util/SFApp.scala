package com.github.glowskir.sparkgp.util

import fuel.util.FApp
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by glowskir on 25.04.16.
  */
trait SApp extends Logging {
  implicit lazy val sqlc = {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local")
      .set("spark.ui.retainedJobs", "100000")
      .set("spark.ui.retainedStages", "100000")
    val newsc = new SparkContext(conf)
    log.warn(conf.toDebugString)
    newsc.setLogLevel("WARN")
    new SQLContext(newsc)
  }
}
