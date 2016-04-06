package com.github.glowskir.sparkgp.func

import com.github.glowskir.sparkgp.core.SparkStatePop
import fuel.func.Initializer
import fuel.util.Options
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
/**
  * Created by glowskir on 05.04.16.
  */
class SparkRandomStatePop[S:ClassTag](solutionGenerator: () => S)(implicit opt: Options, sc: SparkContext)
  extends Initializer[SparkStatePop[S]] with Serializable {
  val populationSize = opt('populationSize, 1000, (_: Int) > 0)

  def apply(x: Unit): SparkStatePop[S] = sc.range(0, populationSize).map(_ => solutionGenerator())
}

object SparkRandomStatePop {
  def apply[S:ClassTag](solutionGenerator: () => S)(implicit opt: Options, sc: SparkContext) = {
    new SparkRandomStatePop(solutionGenerator)

  }
}
