package com.github.glowskir.sparkgp

import fuel.func.RunExperiment
import fuel.moves.BitSetMoves
import fuel.util.FApp
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.BitSet

/**
  * Created by glowskir on 02.04.16.
  */
object MaxOnes1 extends FApp {
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  implicit val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  RunExperiment(SparkSimpleEA(moves = BitSetMoves(100),
    eval = (s: BitSet) => s.size,
    stop = (s: BitSet, e: Int) => e == 0))
}