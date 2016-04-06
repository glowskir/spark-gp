package com.github.glowskir.sparkgp

/**
  * Created by glowskir on 04.04.16.
  */

import com.github.glowskir.sparkgp.core.SparkStatePop
import com.github.glowskir.sparkgp.func._
import com.github.glowskir.sparkgp.moves.SparkMoves
import fuel.func._
import fuel.util.{Collector, Options, TRandom}
import org.apache.spark.SparkContext

import scala.reflect.ClassTag


abstract class SparkEACore[S: ClassTag, E](moves: SparkMoves[S], evaluation: SparkEvaluation[S, E],
                                           stop: (S, E) => Boolean = (s: S, e: E) => false)(
                                            implicit opt: Options, sc: SparkContext)
  extends IterativeSearch[SparkStatePop[(S, E)]] with (() => SparkStatePop[(S, E)]) with Serializable  {

  def initialize: Unit => SparkStatePop[(S, E)] = SparkRandomStatePop(moves.newSolution _) andThen evaluate

  def evaluate: (SparkStatePop[S]) => SparkStatePop[(S, E)] = evaluation andThen report

  override def terminate: Seq[(SparkStatePop[(S, E)]) => Boolean] = SparkTermination(stop).+:(SparkTermination.MaxIter(it))

  def report = (s: SparkStatePop[(S, E)]) => {
    println(f"Gen: ${it.count}");
    s
  }


  def apply(): SparkStatePop[(S, E)] = (initialize andThen algorithm) ()
}


class SparkSimpleEA[S: ClassTag, E](moves: SparkMoves[S],
                                    eval: S => E,
                                    stop: (S, E) => Boolean = (s: S, e: E) => false)(
                                     implicit opt: Options, coll: Collector, rng: TRandom, ordering: Ordering[E])
  extends SparkEACore[S, E](moves, SparkEvaluation(eval), stop)(implicitly, opt, moves.moves.context) {

  def selection: SparkSelection[S, E] = new SparkTournamentSelection[S, E](ordering)

  override def iter: (SparkStatePop[(S, E)]) => SparkStatePop[(S, E)] =
    SparkSimpleBreeder[S, E](selection, moves.moves) andThen evaluate


  val bsf = SparkBestSoFar[S, E](ordering, it)

  override def report: (SparkStatePop[(S, E)]) => SparkStatePop[(S, E)] = bsf
}

object SparkSimpleEA {
  def apply[S: ClassTag, E](moves: SparkMoves[S], eval: S => E)(
    implicit opt: Options, coll: Collector, rng: TRandom, ordering: Ordering[E]) =
    new SparkSimpleEA(moves, eval)

  def apply[S: ClassTag, E](moves: SparkMoves[S], eval: S => E, stop: (S, E) => Boolean)(
    implicit opt: Options, coll: Collector, rng: TRandom, ordering: Ordering[E]) =
    new SparkSimpleEA(moves, eval, stop)

  /** Creates EA that should stop when evaluation reaches certain value */
  def apply[S: ClassTag, E](moves: SparkMoves[S], eval: S => E, optimalValue: E)(
    implicit opt: Options, coll: Collector, rng: TRandom, ordering: Ordering[E]) =
    new SparkSimpleEA(moves, eval, (_: S, e: E) => e == optimalValue)
}
