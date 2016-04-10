package com.github.glowskir.sparkgp.util

/**
  * Created by glowskir on 10.04.16.
  */
case class OrderingTupleBySecond[A, B](implicit ord: Ordering[B]) extends Ordering[(A, B)] {
  override def compare(x: (A, B), y: (A, B)): Int = ord.compare(x._2, y._2)
}
