package com.github.glowskir.sparkgp.util

import org.apache.spark.streaming.Duration

/**
  * Created by glowskir on 17.05.16.
  */
object Timing {
  def time[R](block: => R, name: String = ""): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println(name + ":Elapsed time: " + Duration((t1 - t0) / 1000000).prettyPrint)
    result
  }
}
