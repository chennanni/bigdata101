package max.learn.scala.implicit_code

import java.io.File
import Entity._

object ImplicitAspect {

  // 实际使用中，我们把所有 implicit 定义都放到一个类中，然后使用时只需要引用这个类就行了 -> import ImplicitAspect._

  // implicit method
  implicit def man2superman(man:Man):Superman = new Superman(man.name)
  implicit def file2RichFile(file: File): RichFile = new RichFile(file)

  // implicit class
  implicit class Calculator(x:Int) {
    def add(a:Int) = a + x
  }
}
