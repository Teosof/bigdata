/*
 * Copyright (c) 2020. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package LabOne

import scala.util.Random

object LabOne {
  def main(args: Array[String]): Unit = {
    val array: Seq[Int] = generator(10)
    println(array.mkString("Input Seq(", ", ", ")"))
    val evens: Seq[Int] = array.filter(isEven) //    scala filter
    println(evens.mkString("Output Seq(", ", ", ")"))
  }

  def isEven(number: Int): Boolean = number % 2 == 0

  def generator(N: Int): Seq[Int] = Seq.fill(N)(Random.nextInt(100))
}
