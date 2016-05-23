package com.github.glowskir.sparkgp

/**
  * Created by glowskir on 04.04.16.
  */

import com.github.glowskir.sparkgp.core.{SparkStatePop, _}
import com.github.glowskir.sparkgp.func._
import fuel.func._
import fuel.moves.Moves
import fuel.util.{Collector, Options}
import org.apache.spark.sql.{Encoder, SQLContext}

import scala.reflect.ClassTag

abstract class SparkEACore[S: Encoder, E: Encoder](moves: Moves[S], evaluation: SparkEvaluation[S, E],
                                                   stop: (S, E) => Boolean = (s: S, e: E) => false)(
                                                    implicit opt: Options, sqlc: SQLContext)
  extends IterativeSearch[SparkStatePop[(S, E)]] with (() => SparkStatePop[(S, E)]) with Serializable {
  implicit val clsTagS: ClassTag[S] = implicitly[Encoder[S]].clsTag


  def initialize: Unit => SparkStatePop[(S, E)] = ((_: Unit) => {
    val func: (Unit) => S = (_:Unit) => {moves.newSolution}
    sqlc.createDatasetFromFunction(1000, func).cache()
  }) andThen evaluate

  def evaluate: (SparkStatePop[S]) => SparkStatePop[(S, E)] = evaluation andThen report

  override def terminate: Seq[(SparkStatePop[(S, E)]) => Boolean] = SparkTermination(stop).+:(SparkTermination.MaxIter(it))

  def report = (s: SparkStatePop[(S, E)]) => {
    println(f"Gen: ${it.count}");
    s
  }


  def apply(): SparkStatePop[(S, E)] = (initialize andThen algorithm) ()
}


class SparkSimpleEA[S: Encoder, E: Encoder](moves: Moves[S],
                                            eval: S => E,
                                            stop: (S, E) => Boolean = (s: S, e: E) => false)(
                                             implicit opt: Options, coll: Collector, ordering: Ordering[E], sqlc: SQLContext)
  extends SparkEACore[S, E](moves, SparkEvaluation(eval), stop)(implicitly, implicitly, opt, sqlc) {


  def selection: SparkSelection[S, E] = new SparkTournamentSelection[S, E](ordering)

  override def iter: (SparkStatePop[(S, E)]) => SparkStatePop[(S, E)] =
    SparkSimpleBreeder[S, E](selection,moves) andThen evaluate

  val bsf = SparkBestSoFar[S, E](ordering, it)

  override def report: (SparkStatePop[(S, E)]) => SparkStatePop[(S, E)] = bsf
}

object SparkSimpleEA {
  def apply[S: Encoder, E: Encoder](moves: Moves[S], eval: S => E)(
    implicit opt: Options, coll: Collector, ordering: Ordering[E], sqlc: SQLContext) =
    new SparkSimpleEA(moves, eval)

  def apply[S: Encoder, E: Encoder](moves: Moves[S], eval: S => E, stop: (S, E) => Boolean)(
    implicit opt: Options, coll: Collector, ordering: Ordering[E], sqlc: SQLContext) =
    new SparkSimpleEA(moves, eval, stop)

  /** Creates EA that should stop when evaluation reaches certain value */
  def apply[S: Encoder, E: Encoder](moves: Moves[S], eval: S => E, optimalValue: E)(
    implicit opt: Options, coll: Collector, ordering: Ordering[E], sqlc: SQLContext) =
    new SparkSimpleEA(moves, eval, (_: S, e: E) => e == optimalValue)
}
