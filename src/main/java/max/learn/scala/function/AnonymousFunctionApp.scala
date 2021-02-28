package max.learn.scala.function

/**
  * 匿名函数： 函数是可以命名的，也可以不命名
  * (参数名：参数类型...) => 函数体
  */
object AnonymousFunctionApp extends App {

  println
  println("***** anonymous function: map")
  val l = List(1, 2, 3, 4, 5, 6, 7, 8)
  // map: 逐个去操作集合中的每个元素
  l.map((x: Int) => (x + 1 + ",")).foreach(print)
  println
  l.map((x) => (x *2)).map(_ + ",").foreach(print)
  println
  //l.map(x => x * 2)
  //l.map(_ * 2)
  l.map(_ * 2).filter(_ > 8).map(_ + ",").foreach(print)
  println

  println
  println("***** anonymous function: reduce")
  //1+2=3  3+3=6  6+4=10  10+5=15 ...
  println(l.reduce(_+_)) // 累加
  //l.reduceLeft(_- _)
  //l.reduceRight(_-_)
  println(l.fold(1)(_*_)) // 累乘

}