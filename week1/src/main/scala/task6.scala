object task6 {

  def abs(number: Double) = if (number > 0) number else -number

  def isGoodEnough(approximation: Double, number: Double) = abs(approximation * approximation - number)/number < 0.0001

  def improve(approximation: Double, number: Double) = (approximation + number/approximation)/2

  def newtonsMethod(approximation: Double, x: Double): Double =  {
    if (isGoodEnough(approximation,x)) approximation
    else newtonsMethod(improve(approximation, x), x)
  }

  def sqrt(number: Double): Double  =  newtonsMethod(1, number)
}
