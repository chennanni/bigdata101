package max.learn.scala.implicit_code

object ImplicitValueApp extends App {

  // 定义了一个方法，入参为 implicit String
  // 即如果 context 里面有类型为 String 的 implicit val/var ，即可自动匹配上
  def sayHello(implicit name: String): Unit = {
    println(name + " hello !")
  }

  implicit val name_1 = "Alice"

  // 如果有两个可匹配的值，就会抛 ambiguous implicit value Error
  // implicit var name_2 = "Bob"

  sayHello
  sayHello("Charlie")

}
