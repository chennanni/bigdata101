package max.learn.scala.collection

object FlatMapApp extends App {

  println
  println("***** flatten")
  val f = List(List(1, 2), List(3, 4), List(5, 6))
  println("original list: " + f)
  println("flatten list: " + f.flatten)

  println
  println("***** flatmap")
  println("original map: " + f.map(_.map(_ * 2)))
  println("flatmap: "  + f.flatMap(_.map(_ * 2))) // 先map，后flatten

  println
  println("***** flatmap usage: read file and print")
  val txt = scala.io.Source.fromFile("D:\\codebase\\bigdataez\\data\\scala\\hello.txt").mkString
  val txts = List(txt)
  txts.flatMap(_.split(","))
    .map(x => (x,1))
    .foreach(println)

}
