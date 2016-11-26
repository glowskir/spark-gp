package com.github.glowskir.sparkgp.example

import com.github.glowskir.sparkgp.SparkMigrationEA
import com.github.glowskir.sparkgp.util.SApp
import fuel.func.RunExperiment
import fuel.moves.{BitSetMoves, PermutationMoves}
import fuel.util.{FApp, Options, OptionsMap}

import scala.collection.immutable.BitSet


object MaxOnes1 extends FApp with SApp {
  val maxOnesSize = opt('maxOnesSize, 10000, (_: Int) >= 0)
  RunExperiment(new SparkMigrationEA(
    BitSetMoves(maxOnesSize),
    (s: BitSet) => s.size,
    (s: BitSet, e: Int) => e == 0
  ))
}

object TSPLocal extends FApp with SApp {
  override implicit lazy val opt: OptionsMap = new OptionsMap((Options(Map(('numCities, 300), ('maxGenerations, 50), ('populationSize, 1000))) ++ Options(args)).allOptions)
  val numCities = opt('numCities, (_: Int) > 0)
  val cities = Seq.fill(numCities)((rng.nextDouble, rng.nextDouble))
  val distances = for (i <- cities) yield for (j <- cities)
    yield math.hypot(i._1 - j._1, i._2 - j._2)

  val eval = (s: Seq[Int]) =>
    s.indices.map(i => distances(s(i))(s((i + 1) % s.size))).sum

  RunExperiment(new SparkMigrationEA(PermutationMoves(numCities), eval))
}