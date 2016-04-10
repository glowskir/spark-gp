package com.github.glowskir.sparkgp.func

import fuel.func.SearchOperator
import fuel.util.Options
import org.apache.spark.rdd.RDD

///**
//  * Created by glowskir on 06.04.16.
//  */
//trait SparkSearchOperator[S] extends (Stream[S] => (Seq[S], Stream[S])) {
//  protected def context : SparkContext
//}
//
//class SearchOperator1[S:ClassTag](body: S => S,
//                                  isFeasible: S => Boolean = (_: S) => true)(implicit val context: SparkContext)
//  extends SparkSearchOperator[S] {
//  override def apply(s: Stream[S]) = (context.makeRDD(Seq(body(s.head))).filter(isFeasible(_)), s.tail)
//}
//
//class SearchOperator2[S](body: Function2[S, S, (S, S)],
//                         isFeasible: S => Boolean = (_: S) => true)(implicit val context: SparkContext)
//  extends SparkSearchOperator[S] {
//  override def apply(s: Stream[S]) = {
//    val r = body(s(0), s(1))
//    (List(r._1, r._2).filter(isFeasible(_)), s.drop(2))
//  }
//}
//
//class SearchOperator2_1[S](body: Function2[S, S, S],
//                           isFeasible: S => Boolean = (_: S) => true)(implicit val context: SparkContext)
//  extends SparkSearchOperator[S] {
//  override def apply(s: Stream[S]) = (List(body(s(0), s(1))).filter(isFeasible(_)), s.drop(2))
//}
//
///* A bit verbose because of compiler's complaint "multiple overloaded alternatives of method apply define default arguments. "
// *
// */
//object SearchOperator {
//  def apply[S](body: S => S) = new SearchOperator1[S](body)
//  def apply[S](body: S => S, isFeasible: S => Boolean) =
//    new SearchOperator1[S](body, isFeasible)
//
//  def apply[S](body: Function2[S, S, (S, S)]) = new SearchOperator2[S](body)
//  def apply[S](body: Function2[S, S, (S, S)], isFeasible: S => Boolean) =
//    new SearchOperator2[S](body, isFeasible)
//
//  def apply[S](body: Function2[S, S, S]) = new SearchOperator2_1[S](body)
//  def apply[S](body: Function2[S, S, S], isFeasible: S => Boolean) =
//    new SearchOperator2_1[S](body, isFeasible)
//
//  def Identity[S] = new SearchOperator1[S](identity)
//  def Identity[S](isFeasible: S => Boolean) = new SearchOperator1[S](identity, isFeasible)
//}
//
//
///**
//  * TODO: handle probability options
//  *
//  */
object SparkRandomMultiOperator {
  def apply[S](pipes: RDD[SearchOperator[S]])(implicit opt: Options) = {
    () => pipes.takeSample(true, 1).head
  }
}