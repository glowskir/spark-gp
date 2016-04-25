package com.github.glowskir.sparkgp.util

import fuel.util.FApp
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by glowskir on 25.04.16.
  */
trait SApp {
  implicit lazy val sc = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val newsc = new SparkContext(conf)
    newsc.setLogLevel("WARN")
    newsc
  }
}
