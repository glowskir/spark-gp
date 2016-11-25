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
      (s: SparkStatePop[(S, E)]) => {
        val reference: E = ref()._2
        s.par.map(_.flatMap(es => ord.tryCompare(es._2, reference).map(_ >= 0) ).cache().reduce(_ && _)).reduce(_ && _)
      }
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
  def apply[S, E](otherCond: (S, E) => Boolean = (_: S, _: E) => false)(implicit config: Options) : Seq[SparkStatePop[(S, E)] => Boolean] = Seq(
    MaxTime(config),
    (s: SparkStatePop[(S, E)]) => {
      s.par.map(_.map(es => otherCond(es._1, es._2)).reduce(_ || _)).reduce(_ || _)
    })
}
