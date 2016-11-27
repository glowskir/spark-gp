package com.github.glowskir.sparkgp

import fuel.core.Greatest
import fuel.util._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by glowskir on 04.04.16.
  */

trait SparkSelection[S, E] extends (RDD[(S, E)] => Stream[(S, E)])


class GreedySelection[S, E](implicit o: Ordering[E]) extends SparkSelection[S, E] {

  override def apply(pop: RDD[(S, E)]): Stream[(S, E)] = {
    val best = pop.min()(o.on(_._2))
    Stream.continually(best)
  }
}

abstract class StochasticSelection[S, E] extends SparkSelection[S, E]

class RandomSelection[S, E] extends StochasticSelection[S, E] {
  override def apply(pop: RDD[(S, E)]): Stream[(S, E)] = {
    Stream.consWrapper(apply(pop)).#:::(
      pop.takeSample(withReplacement = true, num = 100).toStream
    )
  }
}


class TournamentSelection[S, E](val ordering: Ordering[E], val tournamentSize: Int, val sizeHint: Int)
  extends StochasticSelection[S, E] {
  implicit private def iordering = ordering

  def this(o: Ordering[E])(implicit opt: Options) =
    this(o, opt('tournamentSize, 7, (_: Int) >= 2), Math.min(opt('populationSize, 10000) / opt('islands, 1) / opt('tournamentSize, 7) / 10 + 1, 10000))

  def apply(pop: RDD[(S, E)]): Stream[(S, E)] = {
    Stream.consWrapper(apply(pop)).#:::(
      pop.takeSample(withReplacement = true, num = tournamentSize * sizeHint).iterator.grouped(tournamentSize).map(_.minBy(_._2)).toStream
    )
  }
}

object TournamentSelection {
  def apply[S, E](o: Ordering[E])(implicit opt: Options) =
    new TournamentSelection[S, E](o)(opt)

  def apply[S, E](opt: Options)(o: Ordering[E]) =
    new TournamentSelection[S, E](o)(opt)
}

/** Partial tournament: the winner is the solution that dominates all the remaining in
  * the pool, or if no such solution exists then a randomly picked solution.
  *
  * Also known as dominance tournament.
  */
class PartialTournament[S, E](val tournamentSize: Int)(implicit ordering: PartialOrdering[E], rng: TRandom)
  extends StochasticSelection[S, E] {

  implicit val ord = new PartialOrdering[(S, E)] {
    override def tryCompare(a: (S, E), b: (S, E)) = ordering.tryCompare(a._2, b._2)

    override def lteq(a: (S, E), b: (S, E)) = ordering.lteq(a._2, b._2)
  }

  def apply(pop: RDD[(S, E)]): Stream[(S, E)] = {
    val rnd = new scala.util.Random()
    Stream.consWrapper(apply(pop)).#:::(
      pop.takeSample(withReplacement = true, num = tournamentSize * 10).iterator.grouped(tournamentSize).map(tournament => {
        Greatest(tournament).getOrElse(tournament(rnd.nextInt(tournament.size)))
      }).toStream
    )
  }
}


/**
  * Fitness-proportionate selection: draws a solution proportionally to its fitness.
  *
  * This is a rather inefficient implementation, as it recalculates the distribution of
  * fitness for entire population in every
  * act of selection. This could be sped up by first normalizing the fitness in the entire
  * population and then drawing a number from [0,1]. However, fitness-proportionate selection
  * has its issues and is not in particularly wide use today, so this simple implementation
  * should be sufficient.
  */
class FitnessPropSelSlow[S: ClassTag] extends SparkSelection[S, Double] {
  def apply(pop: RDD[(S, Double)]) = {
    val rnd = new scala.util.Random()
    val sum = pop.map(_._2).sum()
    val averaged = pop.mapValues(_ / sum).cache().localCheckpoint()
    val partitionData = averaged.mapPartitionsWithIndex((index, it) => {
      Iterator.single(index, it.map(_._2).sum)
    }).collect()
    Stream.continually({
      val desired = sum * rnd.nextDouble()
      var currentSum: Double = 0
      val partition = partitionData.takeWhile(t => {
        currentSum = currentSum + t._2
        currentSum < desired
      }).last
      val start = currentSum - partition._2
      averaged.mapPartitionsWithIndex((index, it) => {
        if (index == partition._1) {
          var partitionSum: Double = start
          val result = it.takeWhile(t => {
            partitionSum = partitionSum + t._2
            partitionSum < desired
          }).toVector.last._1
          Iterator.single((result, partitionSum))
        } else {
          Iterator.empty
        }
      }).collect().head
    })
  }
}