package max.learn.scala.`match`

object ExceptionApp extends App {

  try{
    val i = 10/0
    println(i)
  } catch {
    case e:ArithmeticException => println("catch exception: 除数不能为0..")
    case e:Exception => println(e.getMessage)
  } finally {
    // something
  }
}
