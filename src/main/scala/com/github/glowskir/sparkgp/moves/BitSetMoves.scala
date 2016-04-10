package com.github.glowskir.sparkgp.moves

import org.apache.spark.SparkContext

import scala.collection.immutable.BitSet

/**
  * Created by glowskir on 06.04.16.
  * TODO avoid copy paste
  */
class BitSetMoves(numVars: Int)(implicit val context: SparkContext) extends VectorMoves[BitSet] {
  assert(numVars > 0)
  val rng = new java.util.Random()

  override def newSolution: BitSet = BitSet.empty ++
    (for (i <- 0.until(numVars); if (rng.nextBoolean)) yield i)

  override def onePointMutation = (p: BitSet) => {
    val bitToMutate = rng.nextInt(numVars)
    if (p(bitToMutate)) p - bitToMutate else p + bitToMutate
  }

  override def onePointCrossover = (p1: BitSet, p2: BitSet) => {
    val cuttingPoint = rng.nextInt(numVars)
    val (myHead, myTail) = p1.splitAt(cuttingPoint)
    val (hisHead, hisTail) = p2.splitAt(cuttingPoint)
    (myHead ++ hisTail, hisHead ++ myTail)
  }

  override def twoPointCrossover = (p1: BitSet, p2: BitSet) => {
    val h = (rng.nextInt(numVars), rng.nextInt(numVars))
    val c = if (h._1 <= h._2) h else h.swap
    val (myHead, myRest) = p1.splitAt(c._1)
    val (myMid, myTail) = myRest.splitAt(c._2)
    val (hisHead, hisRest) = p2.splitAt(c._1)
    val (hisMid, hisTail) = myRest.splitAt(c._2)
    (myHead ++ hisMid ++ myTail, hisHead ++ myMid ++ hisTail)
  }
}

object BitSetMoves {
  def apply(numVars: Int)(implicit context: SparkContext) = new BitSetMoves(numVars)
}