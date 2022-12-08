object Main {
  def main(args: Array[String]): Unit = {
    //Task 1
    val s = mySum(20, 21)
    println(s)
    //Task 2
    println(pascal(3, 2))
    //Task 3
    println(balance("a(()".toList))
    //Task 4
    val a = Array(1, 2, 3, 4, 5)
    println(a.map(a=>a*a).sum)
    //task 5
    println("sheena is a punk rocker rocker she is a punk punk".split(" ").map(s => (s, 1)).groupBy(p => p._1).mapValues(v => v.length))
    println("sheena is a punk rocker she is a punk punk".split(" ").map((_, 1)).groupBy(_._1).mapValues(v => v.map(_._2).reduce(_+_)))
    //The functions are mapping each words to its count in the string.
    //task 6
    println(sqrt(2))
    println(sqrt(100))
    println(sqrt(1e-16))
    println(sqrt(1e60))

  }

  def mySum(a: Int, b: Int): Int = {
    return a + b
  }
  def pascal(r: Int, c: Int): Int = {
    if (c == 0 || c == r) 1
    else pascal(r - 1, c - 1) + pascal(r - 1, c)
  }
  def balance(chars: List[Char]): Boolean = {
    def nestedFunction(chars: List[Char], balanceCount: Int): Boolean = {
      if (chars.isEmpty) {
        balanceCount == 0
      } else {
        val headChar = chars.head
        val n =
          if (headChar == '(') balanceCount + 1
          else if (headChar == ')') balanceCount - 1
          else balanceCount
        if (n >= 0) nestedFunction(chars.tail, n)
        else false
      }
    }

    nestedFunction(chars, 0)
  }
  def abs(number: Double) = if (number > 0) number else -number

  def isGoodEnough(approximation: Double, number: Double) = abs(approximation * approximation - number) / number < 0.0001

  def improve(approximation: Double, number: Double) = (approximation + number / approximation) / 2

  def newtonsMethod(approximation: Double, x: Double): Double = {
    if (isGoodEnough(approximation, x)) approximation
    else newtonsMethod(improve(approximation, x), x)
  }

  def sqrt(number: Double): Double = newtonsMethod(1, number)
}