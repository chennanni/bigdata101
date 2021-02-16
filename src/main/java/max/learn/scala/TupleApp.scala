package max.learn.scala

object TupleApp extends App{

  println("***** example 1")
  val a = (1,2,3,4,5)
  for(i <- 0 until(a.productArity)) {
    println(a.productElement(i))
  }

  println("***** example 2")
  val hostPort = ("localhost",8080)
  println(hostPort._1)
  println(hostPort._2)

}