package max.learn.scala.io

import scala.io.Source

object FileApp {

  def main(args: Array[String]): Unit = {

    val file_1 = Source.fromFile("D:\\codebase\\bigdataez\\data\\scala\\hello.txt")(scala.io.Codec.ISO8859)
    val file_2 = Source.fromFile("D:\\codebase\\bigdataez\\data\\scala\\hello.txt")(scala.io.Codec.ISO8859)

    println("***** 1. read by line")
    def readLine(): Unit ={
      for(line <- file_1.getLines()){
        println(line)
      }
    }
    readLine()

    println
    println("***** 2. read by char")
    def readChar(): Unit ={
      for(ele <- file_2) {
        println(ele)
      }
    }
    readChar()

    println
    println("***** 3. read from network")
    def readNet(): Unit ={
      val file = Source.fromURL("http://www.baidu.com")
      for(line <- file.getLines()){
        println(line)
      }
    }
    readNet()

  }
}
