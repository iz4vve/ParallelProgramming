package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def checkParen(chars: Array[Char], openCount: Int): Boolean = {
      if (chars.isEmpty) openCount == 0
      else if (openCount < 0) false
      else if (chars.head == '(') checkParen(chars.tail, openCount + 1)
      else if (chars.head == ')') checkParen(chars.tail, openCount - 1)
      else checkParen(chars.tail, openCount)
    }
    checkParen(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int)  = {
      var index = idx
      var start = 0
      var end = 0
      var switched = false

      while (index < until){
        if (start < 0){
          switched = true
        } else {
          switched = false
        }

        if (chars(index) == '('){
          if (switched){
            end += 1
          } else {
            start += 1
          }

        }
        if (chars(index) == ')')
        {
          if (switched) {
            end -= 1
          } else {
            start -= 1
          }
        }

        index += 1
      }
      (start,  end)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse (from, until)
      else {
        val midPoint = from + (until - from) / 2
        val (p, p2) = parallel(reduce(from, midPoint), reduce(midPoint, until))

        if (p._1 < 0 && p2._1 > 0) (p._1 , p2._1 + p._2 + p2._2)
        else if (p2._1 < 0 && p._2 > 0) (p._1 + p2._1 + p._2 ,  + p2._2)
        else (p._1 + p2._1, p._2 + p2._2)
      }
    }

    val res = reduce(0, chars.length)
    res._1 + res._2 == 0 && res._1 >= 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
