package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var mismatch = true
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
        var i = 0
        var open = 0
        var foundMismatch = false
        
        while (i < chars.length && !foundMismatch) {
          if (chars(i) == '(') open = open + 1
          else if (chars(i) == ')') { 
            if (open == 0) foundMismatch = true
            else open = open - 1
          }
          i = i + 1
        }
        
        if (foundMismatch) false
        else open == 0
    }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int) : (Int, Int) = {
      /*println(s"calling traverse with $idx, $until")*/
      
      var i = idx
      var open = 0
      var close = 0
      
      while (i < until) {
        if (chars(i) == '(') open = open + 1
        if (chars(i) == ')') {
          if (open == 0) close = close + 1
          else open = open - 1
        }
        i = i + 1
      }
      
      (open, close)
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      /*println(s"calling reduce with $from, $until")*/
      
      if (until - from <= threshold) traverse(from, until)
      else {
        val mid = from + (until - from)/2
        val ((leftOpen, leftClose), (rightOpen, rightClose))  = parallel(reduce(from, mid), reduce(mid, until))
        if (leftOpen >= rightClose) (leftOpen - rightClose + rightOpen, leftClose)
        else (rightOpen, leftClose + rightClose - leftOpen)
      }
    }

     reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!
  
}
