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
  val populationSize: Int = opt('populationSize, 10000, (_: Int) % 10 == 0)
  val islands: Int = opt('islands, 1, (_: Int) >= 1)

  def apply(x: Unit): SparkStatePop[S] = {
    val rnd = new scala.util.Random()
    (1 to islands).map(_=> sc.range(0, populationSize/islands).map(UnitFuncToIntFunc(solutionGenerator)))
  }
}

object SparkRandomStatePop {
  def apply[S: ClassTag](solutionGenerator: () => S)(implicit opt: Options, sc: SparkContext) = {
    new SparkRandomStatePop(solutionGenerator)
  }
}
