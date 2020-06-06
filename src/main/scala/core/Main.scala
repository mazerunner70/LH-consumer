package core

import framework.Consumer

class Main {
  def run() = {
    val rawConsumer = Consumer[String, String]("grp1", "776")
    rawConsumer.subscribe("fib", x=>println(s"raw= $x"))
    println("Now waiting1")
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val main = new Main()
    main.run()
  }
}