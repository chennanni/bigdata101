package max.learn.scala.collection

object ArrayApp extends App{

  println("***** 1.1 定长数组")
  val a = new Array[String](5)
  a(1) = "hello"
  println("Array a length: " + a.length)
  println("a(1): " + a(1))

  println("***** 1.2 定长数组")
  val b = Array("hadoop", "spark", "storm")
  println("Array b: " + b)
  println("Array b elements: " + b.mkString(","))

  println("***** 1.3 定长数组")
  val c = Array(2,3,4,5,6,7,8,9)
  println("Array c elements: " + c.mkString(","))
  println("Array c sum: " + c.sum)
  println("Array c min: " + c.min)
  println("Array c max: " + c.max)

  println()
  println("***** 2. 变长数组")
  val d = scala.collection.mutable.ArrayBuffer[Int]()
  d += 1
  d += 2
  d += (3,4,5)
  d ++= Array(6,7,8)
  println("Array d elements: " + d)
  d.insert(0,0)
  println("insert 0: " + d)
  d.remove(0)
  println("remove 0: " + d)

  println()
  println("***** 3.1 遍历")
  for(i <- 0 until c.length) {
    print(c(i) + ",")
  }

  println();
  println("***** 3.2 遍历")
  for(ele <- c) {
    print(ele + ",")
  }

  println();
  println("***** 3.3 反向遍历")
  for(i <- (0 until c.length).reverse) {
    print(c(i) + ",")
  }

}
