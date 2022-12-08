object task2 {
  def pascal(r: Int, c: Int): Int = {
    if (c == 0 || c == r) 1
    else pascal(r - 1, c - 1) + pascal(r - 1, c)
  }
}
