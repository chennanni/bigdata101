package max.learn.scala.collection

import scala.collection.mutable

object MapApp extends App{

  println("***** 1. map")
  val a = Map("lisi" -> 18, "zhangsan" -> 30)
  println(a)

  println
  println("***** 2.1 map iteration")
  for((key,value) <- a) {
    println(key + " : " + value )
  }

  println("***** 2.2 map iteration")
  for((key,_) <- a) {
    println(key + " : " + a.getOrElse(key, 0) )
  }

  println("***** 2.3 map iteration")
  for(key <- a.keySet) {
    println(key + " : " + a.getOrElse(key, 0))
  }

  println("***** 2.4 map iteration")
  for(value <- a.values) {
    println(value)
  }

  println
  println("***** 3. hash map")
  val b = mutable.HashMap[String,Int]()
  println(b.getOrElse("lisi", 0))

}



