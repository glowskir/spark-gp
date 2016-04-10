package com.github.glowskir.sparkgp

/**
  * Created by glowskir on 04.04.16.
  */

import com.github.glowskir.sparkgp.core.SparkStatePop
import com.github.glowskir.sparkgp.func._
import fuel.func._
import fuel.moves.Moves
import fuel.util.{Collector, Options}
import org.apache.spark.SparkContext

import scala.reflect.ClassTag


abstract class SparkEACore[S: ClassTag, E](moves: Moves[S], evaluation: SparkEvaluation[S, E],
                                           stop: (S, E) => Boolean = (s: S, e: E) => false)(
                                            implicit opt: Options, sc: SparkContext)
  extends IterativeSearch[SparkStatePop[(S, E)]] with (() => SparkStatePop[(S, E)]) with Serializable {

  def initialize: Unit => SparkStatePop[(S, E)] = ((_: Unit) => {
    sc.makeRDD(0.until(1000).map(_ => (moves.newSolution _)()))
  }) andThen evaluate

  def evaluate: (SparkStatePop[S]) => SparkStatePop[(S, E)] = evaluation andThen report

  override def terminate: Seq[(SparkStatePop[(S, E)]) => Boolean] = SparkTermination(stop).+:(SparkTermination.MaxIter(it))

  def report = (s: SparkStatePop[(S, E)]) => {
    println(f"Gen: ${it.count}");
    s
  }


  def apply(): SparkStatePop[(S, E)] = (initialize andThen algorithm) ()
}


class SparkSimpleEA[S: ClassTag, E](moves: Moves[S],
                                    eval: S => E,
                                    stop: (S, E) => Boolean = (s: S, e: E) => false)(
                                     implicit opt: Options, coll: Collector, ordering: Ordering[E], sc: SparkContext)
  extends SparkEACore[S, E](moves, SparkEvaluation(eval), stop)(implicitly, opt, sc) {

  def selection: SparkSelection[S, E] = new SparkTournamentSelection[S, E](ordering)


  override def iter: (SparkStatePop[(S, E)]) => SparkStatePop[(S, E)] =
    SparkSimpleBreeder[S, E](selection, moves) andThen evaluate


  val bsf = SparkBestSoFar[S, E](ordering, it)

  override def report: (SparkStatePop[(S, E)]) => SparkStatePop[(S, E)] = bsf
}

object SparkSimpleEA {
  def apply[S: ClassTag, E](moves: Moves[S], eval: S => E)(
    implicit opt: Options, coll: Collector, ordering: Ordering[E], sc: SparkContext) =
    new SparkSimpleEA(moves, eval)

  def apply[S: ClassTag, E](moves: Moves[S], eval: S => E, stop: (S, E) => Boolean)(
    implicit opt: Options, coll: Collector, ordering: Ordering[E], sc: SparkContext) =
    new SparkSimpleEA(moves, eval, stop)

  /** Creates EA that should stop when evaluation reaches certain value */
  def apply[S: ClassTag, E](moves: Moves[S], eval: S => E, optimalValue: E)(
    implicit opt: Options, coll: Collector, ordering: Ordering[E], sc: SparkContext) =
    new SparkSimpleEA(moves, eval, (_: S, e: E) => e == optimalValue)
}
