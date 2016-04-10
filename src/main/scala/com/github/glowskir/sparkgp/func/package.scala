package com.github.glowskir.sparkgp

import com.github.glowskir.sparkgp.core.SparkStatePop

/**
  * Created by glowskir on 05.04.16.
  */
package object func {
  type SparkEvaluation[S, E] = (SparkStatePop[S]) => SparkStatePop[(S, E)]

  object SparkEvaluation {
    def apply[S, E](f: (S) => E) = {
      (sp: SparkStatePop[S]) => sp.map(s => (s, f(s)))
    }
  }

}
