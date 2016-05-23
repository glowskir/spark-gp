package com.github.glowskir.sparkgp.func

/**
  * Created by glowskir on 06.04.16.
  */

import com.github.glowskir.sparkgp.SparkSelection
import com.github.glowskir.sparkgp.core._
import com.github.glowskir.sparkgp.util.Timing
import fuel.func.{RandomMultiOperator, SearchOperator}
import fuel.util.{Options, Random}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder}

import scala.annotation.tailrec
import scala.collection.AbstractIterator
import scala.reflect.ClassTag


/**
  * TODO: more performant version?
  *
  */
class Breeder[S: Encoder, E: Encoder](val sel: SparkSelection[S, E],
                              val searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E]) {

  implicit val clsTagS: ClassTag[S] = implicitly[Encoder[S]].clsTag
//  def selStream(src: RDD[(S, E)]): Stream[S] = sel(src)._1 #:: selStream(src)
  def selStream(src: RDD[(S, E)]): Stream[S] = {
    Stream.consWrapper(selStream(src)).#:::(
      src.takeSample(true, 7 * 10).iterator.grouped(7).map(_.minBy(_._2)).map(_._1).toStream)
  }

  def breedn(n: Int, s: SparkStatePop[(S, E)]): SparkStatePop[S] = {
    @tailrec def breed(parStream: Stream[S], result: RDD[S] = s.rdd.context.emptyRDD[S], offspring: Seq[S] = List.empty, offspringCount: Int = 0): RDD[S] =
      if (offspringCount >= n)
        result.union(s.rdd.context.makeRDD(offspring)).zipWithIndex().filter(_._2 < n).map(_._1)
      else {
        val (off, parentTail) = searchOperator()(parStream)
        val newOffspringCandidate = offspring ++ off
        val newOffspringCount = offspringCount + off.length
        val (newOffspring, newResult) = if (newOffspringCandidate.length > 10000) {
          (List.empty, result.union(result.context.makeRDD(newOffspringCandidate)).cache())
        } else {
          (newOffspringCandidate, result)
        }
        breed(parentTail, newResult, newOffspring, newOffspringCount)
      }

    val breeded = breed(selStream(s.rdd)).localCheckpoint()
    s.sqlContext.createDataset(breeded.repartition(8)).cache()
  }
}

trait GenerationalBreeder[S, E] extends (SparkStatePop[(S, E)] => SparkStatePop[S])

class SparkSimpleBreeder[S: Encoder, E: Encoder](override val sel: SparkSelection[S, E],

                                     override val searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E])
  extends Breeder[S, E](sel, searchOperator) with GenerationalBreeder[S, E] {

  override def apply(s: SparkStatePop[(S, E)]) = breedn(s.count().toInt, s)
}

object SparkSimpleBreeder {

  def apply[S: Encoder, E: Encoder](sel: SparkSelection[S, E],
                            searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E]) = new SparkSimpleBreeder[S, E](sel, searchOperator)

  def apply[S: Encoder, E: Encoder](sel: SparkSelection[S, E], searchOperators: Seq[SearchOperator[S]])(implicit config: Options,ord: Ordering[E]) =
    new SparkSimpleBreeder[S, E](sel, RandomMultiOperator(searchOperators: _*)(config, new Random()))
}


//class SparkTournamentBreeder[S: Encoder, E: Encoder](val searchOperator: () => SearchOperator[S])(implicit config: Options, ord: Ordering[E])
//  extends GenerationalBreeder[S, E] {
//  implicit val clsTagS: ClassTag[S] = implicitly[Encoder[S]].clsTag
//
//  override def apply(s: SparkStatePop[(S, E)]): SparkStatePop[S] = {
//    val result: SparkStatePop[S] = Timing.time {
//      _apply(s)
//    }
//    result
//  }
//
//  def _apply(s: SparkStatePop[(S, E)]): Dataset[S] = {
//    val srdd = s.rdd.repartition(8).cache()
//    val columns = 7
//    val size = srdd.count()
//    var result: RDD[S] = null
//    var tooSmall = true
//    val input: RDD[(S, E)] = srdd.union(srdd).cache()
//    while (tooSmall) {
//      val partial = Timing.time({
//        partialBreed(input, columns, size * 2)
//      }, "partialBreed")
//      if (result eq null) {
//        result = partial
//      } else {
//        result = result.union(partial).repartition(8).cache()
//      }
//      tooSmall = result.count() < size
//    }
//    val breeded = result.zipWithIndex().filter(_._2 < size).map(_._1)
//    breeded.localCheckpoint()
//    s.sqlContext.createDataset(breeded.repartition(8)).cache()
//  }
//
//  def partialBreed(s: RDD[(S, E)], columns: Int, size: Long): RDD[S] = {
//    val safeOrd = ord
//    val rdds = {
//      0.until(columns).map(column => {
//        s.mapPartitions(originalIterator => {
//          val rand = new scala.util.Random()
//          originalIterator.map(t => {
//            (rand.nextLong() % size, t)
//          })
//        }, true).sortBy(_._1).map(t => Seq(t._2))
//          .zipWithIndex().map(_.swap).cache()
//      })
//    }
//    val op = searchOperator()
//    val tournaments = rdds.reduce(_.join(_).map(t => (t._1, Seq.empty ++ t._2._1 ++ t._2._2))).map(_._2).cache()
//    val victors: RDD[S] = tournaments.map(_.minBy(_._2)(safeOrd)._1).cache()
//    victors.mapPartitions(_.grouped(2).filter(_.size == 2)).flatMap(chosen => op(chosen.toStream)._1).cache()
//  }
//}
//
//object SparkTournamentBreeder {
//  def apply[S: Encoder, E: Encoder](searchOperator: () => SearchOperator[S])(implicit config: Options, ord: Ordering[E]): SparkTournamentBreeder[S, E] =
//    new SparkTournamentBreeder[S, E](searchOperator)
//
//  def apply[S: Encoder, E: Encoder](searchOperators: Seq[SearchOperator[S]])(implicit config: Options, ord: Ordering[E]) =
//    new SparkTournamentBreeder[S, E](RandomMultiOperator(searchOperators: _*)(config, new Random()))
//
//}