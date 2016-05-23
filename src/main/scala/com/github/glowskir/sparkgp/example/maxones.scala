package com.github.glowskir.sparkgp.example

import com.github.glowskir.sparkgp.SparkSimpleEA
import com.github.glowskir.sparkgp.util.{SApp, Timing}
import fuel.func.{RunExperiment, SimpleEA}
import fuel.moves.BitSetMoves
import fuel.util.{FApp, IApp}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.immutable.BitSet


object MaxOnes1 extends FApp with SApp {

  import sqlc.implicits._

  implicit val ens: Encoder[BitSet] = Encoders.javaSerialization(classOf[BitSet])

  RunExperiment(SparkSimpleEA(
    BitSetMoves(100),
    (s: BitSet) => s.size,
    (s: BitSet, e: Int) => e == 0
  ))
}

object Test extends FApp with SApp {

  import sqlc.implicits._

  implicit val ens: Encoder[BitSet] = Encoders.javaSerialization(classOf[BitSet])
  val s = sqlc.sparkContext.makeRDD(0.until(1000).map(BitSet(1,2,3))).repartition(8).cache()
  val size = s.count()
  var results: List[Any] = List()
  while (true) {
    val rdds = {
      0.until(7).map(column => {
        Timing.time({
          s.mapPartitions(originalIterator => {
            val rand = new scala.util.Random()
            originalIterator.map(t => {
              (rand.nextLong() % size, t)
            })
          }, true).sortBy(_._1).map(t => Seq(t._2))
        }, s"rdd $column").zipWithIndex().map(_.swap)
      })
    }
    results = results.++:(rdds)
  }


}


//object MaxOnes2 extends IApp('numVars -> 500, 'maxGenerations -> 200,
//  'printResults -> true) with SApp {
//  RunExperiment(SparkSimpleEA(
//    moves = BitSetMoves(opt('numVars, (_: Int) > 0)),
//    eval = (s: BitSet) => s.size,
//    optimalValue = 0))
//}
//
//
//object MaxOnes3 extends FApp with SApp{
//  RunExperiment(SparkSimpleEA(BitSetMoves(100), (s: BitSet) => s.size, 0))
//}