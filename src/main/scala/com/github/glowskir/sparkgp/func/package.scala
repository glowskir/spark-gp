package com.github.glowskir.sparkgp

import com.github.glowskir.sparkgp.core.SparkStatePop
import org.apache.spark.sql.{Encoder, Encoders}

/**
  * Created by glowskir on 05.04.16.
  */
package object func {
  type SparkEvaluation[S, E] = (SparkStatePop[S]) => SparkStatePop[(S, E)]

  object SparkEvaluation {
    def apply[S:Encoder, E:Encoder](f: (S) => E) = {
      implicit val ent: Encoder[(S, E)] = Encoders.tuple(implicitly[Encoder[S]], implicitly[Encoder[E]])
      (sp: SparkStatePop[S]) => sp.map(s => (s, f(s))).cache()
    }
  }

}
