package max.learn.scala.function

object FunctionApp {

  def main(args: Array[String]): Unit = {

    println("***** 1. test add function")
    println(add(1, 2))

    println("***** 2. test three function - 没有入参的函数，调用时括号是可以省略的")
    println(three())
    println(three)

    println("***** 3. test sayHello function - 同名函数，不同入参")
    sayHello
    sayHello("zhangsan")

    println("***** 4. test loadConf function - 默认入参值")
    loadConf()
    loadConf("spark-production.conf")

    println("***** 5. test speed function - 命名参数")
    println(speed(100, 10))
    println(speed(distance = 100, time = 10))
    println(speed(time = 10, distance = 100))

    println("***** 6. test sum function - 可变长度入参")
    println(sum(1,2))
    println(sum(1,2,3))
    println(sumx(1,2,3,4))

  }

  def add(x:Int, y:Int):Int = {
    x + y
  }

  def three() = 1 + 2

  def sayHello(): Unit = {
    println("Say hello")
  }

  def sayHello(name:String): Unit = {
    println("Say hello: " + name)
  }

  def loadConf(conf:String = "spark-defaults.conf"): Unit = {
    println(conf)
  }

  def speed(distance:Float, time:Float):Float = {
    distance/time
  }


  def sum(a:Int, b:Int) = {
    a + b
  }

  def sum(a:Int, b:Int, c:Int) = {
    a + b + c
  }

  def sumx(numbers:Int*) = {
    var result = 0
    for(number <- numbers) {
      result += number
    }
    result
  }

}
