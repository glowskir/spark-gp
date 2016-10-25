package com.github.glowskir.sparkgp.func

/**
  * Created by glowskir on 06.04.16.
  */

import com.github.glowskir.sparkgp.SparkSelection
import com.github.glowskir.sparkgp.core._
import com.github.glowskir.sparkgp.util.OrderingTupleBySecond
import fuel.func.{RandomMultiOperator, SearchOperator}
import fuel.util.{Options, Random}
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.collection.{SortedSet, mutable}
import scala.reflect.ClassTag


/**
  * TODO: more performant version?
  *
  */
class Breeder[S: ClassTag, E: ClassTag](val sel: SparkSelection[S, E],
                                        val searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E]) {
  def breedn(n: Int, s: SparkStatePop[(S, E)]): SparkStatePop[S] = {
    @tailrec def breed(parStream: Stream[S], result: RDD[S], offspring: Seq[S] = List.empty, offspringCount: Int = 0): RDD[S] =
      if (offspringCount >= n)
        result.union(result.context.makeRDD(offspring)).zipWithIndex().filter(_._2 < n).map(_._1)
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
    s.map(s => {
      val breeded = breed(sel(s).map(_._1), s.context.emptyRDD).localCheckpoint()
      breeded.repartition(8).cache()
    })
  }
}

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

abstract class MigrationBreeder[S: ClassTag, E: ClassTag](override val sel: SparkSelection[S, E],
                                                          override val searchOperator: () => SearchOperator[S],
                                                          val generation: Int)(implicit config: Options, ord: Ordering[E])
  extends Breeder[S, E](sel, searchOperator) with GenerationalBreeder[S, E] with Topology[S, E] {

  val migrationInterval = config("migrationInterval", 1, (x: Int) => {
    x > 0
  })
  val migrationSize = config("migrationSize", 10, (x: Int) => {
    x > 0
  })


  override def apply(s: SparkStatePop[(S, E)]): SparkStatePop[S] = {

    val migrated = if (migrationInterval != 0 && generation % migrationInterval == 0) {
      val exchangeTopology = createExchangeTopology(s)

      @tailrec
      def applyExchange(s: SparkStatePop[(S, E)], exchanges: Seq[(Int, Int)]): SparkStatePop[(S, E)] = {
        exchanges match {
          case Seq() => s
          case exchange +: otherExchanges =>
            val a = s(exchange._1)
            val b = s(exchange._2)
            val size: Long = Math.min(Math.min(a.count(), b.count()), migrationSize)
            val topA: Set[(S, E)] = a.top(size.toInt)(OrderingTupleBySecond[S, E]()(ord)).toSet
            val topB: Set[(S, E)] = b.top(size.toInt)(OrderingTupleBySecond[S, E]()(ord)).toSet
            val newA = a.filter(x => !topA.contains(x)).union(a.context.makeRDD(topB.toList))
            val newB = b.filter(x => !topB.contains(x)).union(a.context.makeRDD(topA.toList))
            val newS: Seq[RDD[(S, E)]] = s.updated(exchange._1, newA).updated(exchange._2, newB)
            applyExchange(newS, otherExchanges)
        }
      }
      applyExchange(s, exchangeTopology).map(_.localCheckpoint())
    } else {
      s
    }
    breedn(migrated.map(_.count()).sum.toInt, migrated)
  }

}

trait GenerationalBreeder[S, E] extends (SparkStatePop[(S, E)] => SparkStatePop[S])

final class SparkSimpleBreeder[S: ClassTag, E: ClassTag](override val sel: SparkSelection[S, E],

                                                         override val searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E])
  extends Breeder[S, E](sel, searchOperator) with GenerationalBreeder[S, E] {

  override def apply(s: SparkStatePop[(S, E)]) = breedn(s.map(_.count()).sum.toInt, s)
}

object SparkSimpleBreeder {

  def apply[S: ClassTag, E: ClassTag](sel: SparkSelection[S, E],
                                      searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E]) = new SparkSimpleBreeder[S, E](sel, searchOperator)

  def apply[S: ClassTag, E: ClassTag](sel: SparkSelection[S, E], searchOperators: Seq[SearchOperator[S]])(implicit config: Options, ord: Ordering[E]) =
    new SparkSimpleBreeder[S, E](sel, RandomMultiOperator(searchOperators: _*)(config, new Random()))
}