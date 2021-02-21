package max.learn.scala.basic

// apply 相当于 constructor 方法，创建 object/class 的时候会被调用
object ApplyApp {

  def main(args: Array[String]): Unit = {
    println("***** 1. object是一个单例对象")
    for(i <- 1 to 10) {
      ApplyTest.incr
    }
    println("counter: " + ApplyTest.count)

    println
    println("***** 2. Object.apply")
    val b = ApplyTest()

    println
    println("***** 3. Class.apply")
    val c = new ApplyTest()
    c()
  }

  class ApplyTest{
    def apply() = {
      println("Class ApplyTest apply....")
    }
  }

  object ApplyTest{
    println("Object ApplyTest enter....")

    var count = 0

    def incr = {
      count = count + 1
      println("incr: " + count)
    }

    // 最佳实践：在Object的apply方法中去new Class
    def apply():ApplyTest = {
      println("Object ApplyTest apply....")
      // 在object中的apply中new class
      new ApplyTest
    }

    println("Object ApplyTest leave....")
  }

}