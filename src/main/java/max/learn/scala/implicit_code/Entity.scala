package max.learn.scala.implicit_code

import java.io.File

object Entity {

  class Man(val name: String) {
    def eat(): Unit = {
      println(s"man[ $name ] eat ..... ")
    }
  }

  class Superman(val name: String) {
    def fly(): Unit = {
      println(s"superman[ $name ] fly ..... ")
    }
  }

  class RichFile(val file: File) {
    def read() = {
      scala.io.Source.fromFile(file.getPath).mkString
    }
  }

}
