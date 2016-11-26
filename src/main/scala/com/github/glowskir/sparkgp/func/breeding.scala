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
import scala.reflect.ClassTag


/**
  * TODO: more performant version?
  *
  */
object Breeder {

  def createBreedResult[S: ClassTag](result: RDD[S], offspring: Seq[S], n: Int) = {
    result.union(result.context.makeRDD(offspring)).zipWithIndex().filter(_._2 < n).map(_._1)
  }
}

class Breeder[S: ClassTag, E: ClassTag](val sel: SparkSelection[S, E],
                                        val searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E]) {

  import Breeder._

  def breedn(n: Int, s: RDD[(S, E)]): RDD[S] = {
    @tailrec def breed(parStream: Stream[S], result: RDD[S], offspring: Seq[S] = List.empty, offspringCount: Int = 0): RDD[S] =
      if (offspringCount >= n)
        createBreedResult(result, offspring, n)
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

    val breeded = breed(sel(s).map(_._1), s.context.emptyRDD).localCheckpoint()
    breeded.localCheckpoint()
  }

}

object MigrationBreeder {
  def removeTopItems[S: ClassTag, E: ClassTag](rdd: RDD[(S, E)], count: Int)(implicit ord: Ordering[E]) = {
    val top = rdd.top(count)(OrderingTupleBySecond[S, E]()(ord)).toSet
    (top, rdd.filter(x => !top.contains(x)))
  }

  def applyMigrations[S: ClassTag, E: ClassTag](s: SparkStatePop[(S, E)], migrationPercent: Int, topology: Seq[(Int, Int)])(implicit ord: Ordering[E]) = {
    var result = s
    var exchanges = topology
    while (exchanges.nonEmpty) {
      val ((exchange: (Int, Int)) +: (otherExchanges: Seq[(Int, Int)])) = exchanges
      exchanges = otherExchanges
      val a = s(exchange._1)
      val b = s(exchange._2)
      val size: Long = Math.max(Math.round(Math.min(a.count() * migrationPercent / 100.0f, b.count() * migrationPercent / 100.0f)), 1)
      val (topA, remainingA) = removeTopItems(a, size.toInt)
      val (topB, remainingB) = removeTopItems(b, size.toInt)

      val newA = remainingA.union(a.context.makeRDD(topB.toList)).localCheckpoint()
      val newB = remainingB.union(a.context.makeRDD(topA.toList)).localCheckpoint()
      result = s.updated(exchange._1, newA).updated(exchange._2, newB)
      s
    }
    result
  }
}

abstract class MigrationBreeder[S: ClassTag, E: ClassTag](override val sel: SparkSelection[S, E],
                                                          override val searchOperator: () => SearchOperator[S],
                                                          val generation: Int)(implicit config: Options, ord: Ordering[E])
  extends Breeder[S, E](sel, searchOperator) with GenerationalBreeder[S, E] with Topology[S, E] {

  val migrationInterval : Int = config("migrationInterval", 5, (x: Int) => {
    x > 0
  })
  val migrationPercent : Int = config("migrationPercent", 5, (x: Int) => {
    x > 0
  })


  override def apply(s: SparkStatePop[(S, E)]): SparkStatePop[S] = {
    val migrated = if (migrationInterval != 0 && generation % migrationInterval == 0) {
      MigrationBreeder.applyMigrations(s, migrationPercent, createExchangeTopology(s))
    } else {
      s
    }
    migrated.toList.par.map(island => breedn(island.count().toInt, island)).toList
  }

}

trait GenerationalBreeder[S, E] extends (SparkStatePop[(S, E)] => SparkStatePop[S])

final class SparkSimpleBreeder[S: ClassTag, E: ClassTag](override val sel: SparkSelection[S, E],

                                                         override val searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E])
  extends Breeder[S, E](sel, searchOperator) with GenerationalBreeder[S, E] {

  override def apply(s: SparkStatePop[(S, E)]) = s.toList.par.map(island => breedn(island.count().toInt, island)).toList
}

object SparkSimpleBreeder {

  def apply[S: ClassTag, E: ClassTag](sel: SparkSelection[S, E],
                                      searchOperator: () => SearchOperator[S])(implicit ord: Ordering[E]) = new SparkSimpleBreeder[S, E](sel, searchOperator)

  def apply[S: ClassTag, E: ClassTag](sel: SparkSelection[S, E], searchOperators: Seq[SearchOperator[S]])(implicit config: Options, ord: Ordering[E]) =
    new SparkSimpleBreeder[S, E](sel, RandomMultiOperator(searchOperators: _*)(config, new Random()))
}