package com.github.glowskir.sparkgp.example

import com.github.glowskir.sparkgp.SparkSimpleEA
import com.github.glowskir.sparkgp.util.SApp
import fuel.func.{RunExperiment, SimpleEA}
import fuel.moves.BitSetMoves
import fuel.util.{FApp, IApp}

import scala.collection.immutable.BitSet


object MaxOnes1 extends FApp with SApp {
  RunExperiment(SparkSimpleEA(
    BitSetMoves(100),
    (s: BitSet) => s.size,
    (s: BitSet, e: Int) => e == 0
  ))
}


object MaxOnes2 extends IApp('numVars -> 500, 'maxGenerations -> 200,
  'printResults -> true) with SApp {
  RunExperiment(SparkSimpleEA(
    moves = BitSetMoves(opt('numVars, (_: Int) > 0)),
    eval = (s: BitSet) => s.size,
    optimalValue = 0))
}


object MaxOnes3 extends FApp with SApp{
  RunExperiment(SparkSimpleEA(BitSetMoves(100), (s: BitSet) => s.size, 0))
}