object task3 {
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
}
