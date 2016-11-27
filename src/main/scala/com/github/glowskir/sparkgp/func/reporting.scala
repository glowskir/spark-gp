package com.github.glowskir.sparkgp.func

import com.github.glowskir.sparkgp.core.SparkStatePop
import com.github.glowskir.sparkgp.util.OrderingTupleBySecond
import fuel.util.{Collector, Counter, Options}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by glowskir on 06.04.16.
  */
class SparkBestSoFar[S, E](opt: Options, coll: Collector, o: Ordering[E], cnt: Counter)
  extends ((SparkStatePop[(S, E)]) => SparkStatePop[(S, E)]) {
  protected var best: Option[(S, E)] = None

  def bestSoFar = best

  val snapFreq = opt('snapshotFrequency, 0)
  val saveBestSoFar = opt('saveBestSoFar, false)

  def apply(s: SparkStatePop[(S, E)]) = {
    Future {
      val bestOfGen = s.par.map(_.min()(OrderingTupleBySecond[S, E]()(o))).min(OrderingTupleBySecond[S, E]()(o))
      if (bestSoFar.isEmpty || o.lt(bestOfGen._2, best.get._2)) {
        best = Some(bestOfGen)
        updateBest(s)
      }
      println(f"Gen: ${cnt.count}  BestSoFar: ${bestSoFar.get}")
      if (snapFreq > 0 && cnt.count % snapFreq == 0)
        coll.saveSnapshot(f"${cnt.count}%04d")
    }
    s
  }

  def updateBest(state: SparkStatePop[(S, E)]) = {
    coll.setResult("best.generation", cnt.count)
    coll.setResult("best.eval",
      if (bestSoFar.isDefined) bestSoFar.get._2 else "NaN")
    coll.setResult("best", bestSoFar.get._1.toString)
    if (saveBestSoFar) coll.write("best", bestSoFar)
    state
  }
}

object SparkBestSoFar {
  def apply[S, E](o: Ordering[E], cnt: Counter)(implicit opt: Options, coll: Collector) =
    new SparkBestSoFar[S, E](opt, coll, o, cnt)
}

