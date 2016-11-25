package com.github.glowskir.sparkgp.func

import com.github.glowskir.sparkgp.core.SparkStatePop
import fuel.util.Random


trait Topology[S, E] {
  def createExchangeTopology(s: SparkStatePop[(S, E)]): Seq[(Int, Int)]
}

trait RandomTopology[S, E] extends Topology[S, E] {
  override def createExchangeTopology(s: SparkStatePop[(S, E)]): Seq[(Int, Int)] = {
    val shuffled = new Random().shuffle(s.indices.toList)
    shuffled.zip(shuffled.tail)
  }
}

trait RingTopology[S, E] extends Topology[S, E] {
  override def createExchangeTopology(s: SparkStatePop[(S, E)]): Seq[(Int, Int)] = {
    s.indices.zip(s.indices.tail)
  }
}