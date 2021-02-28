package max.learn.scala.implicit_code

import java.io.File
import Entity._

// 隐式转换函数
object ImplicitMethodApp {

  println("***** implicit method 1: man can use superman's method fly()")
  implicit def man2superman(man:Man):Superman = new Superman(man.name)
  val man = new Man("Alice")
  man.fly()

  println
  println("***** implicit method 2: file can use RichFile's method read()")
  implicit def file2RichFile(file: File): RichFile = new RichFile(file)
  val file = new File("D:\\codebase\\bigdataez\\data\\scala\\hello.txt")
  val txt = file.read()
  println(txt)

}
