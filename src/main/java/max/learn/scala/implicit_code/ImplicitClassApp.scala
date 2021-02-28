package max.learn.scala.implicit_code

// 隐式转换类
object ImplicitClassApp extends App {

  // 定义了一个 Calculator 隐式转换类
  // 定义了一个 add 方法
  implicit class Calculator(x:Int) {
    def add(a:Int) = a + x
  }

  // 所有 Int 元素可以使用以上定义的 add 方法
  println(1.add(2))

}