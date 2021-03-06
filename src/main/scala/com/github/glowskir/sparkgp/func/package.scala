package com.github.glowskir.sparkgp

import com.github.glowskir.sparkgp.core.SparkStatePop

import scala.reflect.ClassTag

/**
  * Created by glowskir on 05.04.16.
  */
package object func {
  type SparkEvaluation[S, E] = (SparkStatePop[S]) => SparkStatePop[(S, E)]

  object SparkEvaluation {
    def apply[S: ClassTag, E](f: (S) => E) = {
      (sp: SparkStatePop[S]) => sp.toList.par.map(i => i.map(s => (s, f(s))).localCheckpoint()).toList.toSeq
    }
  }

}
