package com.github.glowskir.sparkgp.func

/**
  * Created by glowskir on 06.04.16.
  */

import com.github.glowskir.sparkgp.SparkSelection
import com.github.glowskir.sparkgp.core._
import fuel.func.SearchOperator
import fuel.util.{Options, TRandom}
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.reflect.ClassTag


/**
  * TODO: more performant version?
  *
  */
class Breeder[S: ClassTag, E](val sel: SparkSelection[S, E],
                              val searchOperator: () => SearchOperator[S]) {

  def selStream(src: RDD[(S, E)]): Stream[S] = sel(src)._1 #:: selStream(src)

  def breedn(n: Int, s: RDD[(S, E)]): RDD[S] = {
    @tailrec def breed(parStream: Stream[S], result: RDD[S] = s.context.emptyRDD[S], offspring: Seq[S] = List.empty, offspringCount: Int = 0): RDD[S] =
      if (offspringCount >= n)
        s.context.makeRDD(offspring.take(n)) // this is utterly stupid way of making it
      else {
        val (off, parentTail) = searchOperator()(parStream)
        val newOffspringCandidate = offspring ++ off
        val newOffspringCount = offspringCount + off.length
        val (newOffspring, newResult) = if (newOffspringCandidate.length > 10000) {
          (List.empty, result.union(result.context.makeRDD(newOffspringCandidate)))
        } else {
          (newOffspringCandidate, result)
        }
        breed(parentTail, newResult, newOffspring, newOffspringCount)
      }

    breed(selStream(s))
  }
}

trait GenerationalBreeder[S, E] extends (SparkStatePop[(S, E)] => SparkStatePop[S])

class SparkSimpleBreeder[S: ClassTag, E](override val sel: SparkSelection[S, E],
                                         override val searchOperator: () => SearchOperator[S])
  extends Breeder[S, E](sel, searchOperator) with GenerationalBreeder[S, E] {

  override def apply(s: SparkStatePop[(S, E)]) = breedn(s.count().toInt, s)
}

object SparkSimpleBreeder {

  def apply[S: ClassTag, E](sel: SparkSelection[S, E],
                            searchOperator: () => SearchOperator[S]) = new SparkSimpleBreeder[S, E](sel, searchOperator)

  def apply[S: ClassTag, E](sel: SparkSelection[S, E], searchOperators: RDD[SearchOperator[S]])(
    implicit config: Options, rng: TRandom) =
    new SparkSimpleBreeder[S, E](sel, SparkRandomMultiOperator(searchOperators))
}