package com.github.glowskir.sparkgp.func

import com.github.glowskir.sparkgp.core.SparkStatePop
import fuel.func.Initializer
import fuel.util.Options
import org.apache.spark.SparkContext

import scala.collection.SortedSet
import scala.reflect.ClassTag

/**
  * Created by glowskir on 05.04.16.
  */

private final case class UnitFuncToIntFunc[S](unitf: () => S) extends ((Long) => S) {
  override def apply(v1: Long): S = unitf()
}


class SparkRandomStatePop[S: ClassTag](solutionGenerator: () => S)(implicit opt: Options, sc: SparkContext)
  extends Initializer[SparkStatePop[S]] with Serializable {
  val populationSize = opt('populationSize, 1000, (_: Int) % 10 == 0)
  val islands = opt('islands, 2, (_: Int) >= 1)

  def apply(x: Unit): SparkStatePop[S] = {
    val rnd = new scala.util.Random()
    var boundaries = SortedSet(0, populationSize)
    for (_ <- (1 to (islands - 1))) {
      val oldSize = boundaries.size
      do {
        boundaries = boundaries + (rnd.nextInt(populationSize - 1) + 1)
      } while (boundaries.size == oldSize)
    }
    val boundariesList = boundaries.toList
    boundariesList.zip(boundariesList.tail).map(t => t._2 - t._1).map(population => {
      sc.range(0, population).map(UnitFuncToIntFunc(solutionGenerator))
    })
  }
}

object SparkRandomStatePop {
  def apply[S: ClassTag](solutionGenerator: () => S)(implicit opt: Options, sc: SparkContext) = {
    new SparkRandomStatePop(solutionGenerator)
  }
}
