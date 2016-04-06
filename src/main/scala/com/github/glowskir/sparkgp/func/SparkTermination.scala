package com.github.glowskir.sparkgp.func

import com.github.glowskir.sparkgp.core.SparkStatePop
import fuel.util.{Counter, Options}

/**
  * Created by glowskir on 05.04.16.
  */
object SparkTermination {

  object MaxTime {
    def apply(opt: Options) = {
      val maxMillisec = opt.paramInt("maxTime", 86400000, _ > 0)
      val startTime = System.currentTimeMillis()
      def timeElapsed = System.currentTimeMillis() - startTime
      s: Any => timeElapsed > maxMillisec
    }
  }
  class NoImprovement[S, E] {
    def apply(ref: () => (S, E))(implicit ord: PartialOrdering[E]) = {
      (s: SparkStatePop[(S, E)]) => s.flatMap(es => ord.tryCompare(es._2, ref()._2).map(_ >= 0) ).reduce(_ && _)
    }
  }

  class Count {
    def apply(cnt: Counter, max: Long) = {
      s: Any => cnt.count >= max
    }
  }
  object MaxIter extends Count {
    def apply[S](cnt: Counter)(implicit opt: Options) = {
      val maxGenerations = opt('maxGenerations, 50, (_: Int) > 0)
      super.apply(cnt, maxGenerations)
    }
  }
  def apply[S, E](otherCond: (S, E) => Boolean = (_: S, _: E) => false)(implicit config: Options) = Seq(
    MaxTime(config),
    (s: SparkStatePop[(S, E)]) => s.map(es => otherCond(es._1, es._2)).reduce(_ || _))
}
