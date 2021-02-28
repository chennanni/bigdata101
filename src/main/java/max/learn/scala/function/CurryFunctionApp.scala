package max.learn.scala.function

object CurryFunctionApp extends App {

  println("***** currying function")
  // 一个函数，接收两个参数
  def sum(a:Int, b:Int) = a + b
  println("normal function: " + sum(2,3))

  // 两个函数，各自接收一个参数
  def sum2(a:Int)(b:Int) = a + b
  println("currying function: " + sum2(2)(3))

}
