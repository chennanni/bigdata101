package max.learn.scala.function

import scala.util.Random

/**
  * 偏函数：被包在花括号内没有match的一组case语句
  */
object PartialFunctionApp extends App {

  val names = Array("Alice", "Bob")
  val name = names(Random.nextInt(names.length))

  // match case的写法
  name match {
    case "Alice" =>  println("alice...")
    case "Bob"  => println("bob....")
    case _ => println("not found....")
  }

  // Partial Function的写法
  // 入参[String,String]：A 输入参数类型；B 输出参数类型
  def findName:PartialFunction[String,String] = {
    case "Alice" => "alice..."
    case "Bob"  => "bob...."
    case _ => "not found...."
  }
  println(findName("Alice"))

}
