package max.learn.scala.basic

object StringApp extends App {

  val s = "Hello"
  val name = "Alice"
  println(s + name)
  println(s"Hello$name")

  val b =
    """
      |这是一个多行字符串
      |hello
      |world
      |alice
    """.stripMargin
  println(b)

}
