package max.learn.scala.`match`

import scala.util.Random

object MatchApp extends App {

  println("***** 1.1 match 基本用法")
  val names = Array("Alice", "Bob", "Charlie")
  val name = names(Random.nextInt(names.length))

  name match {
    case "Alice" =>  println("this is alice ...")
    case "Bob"  => println("this is bob ....")
    case _ => println("....")
  }

  println
  println("***** 1.2 双重过滤")
  def judgeGrade(name:String, grade:String): Unit = {
    grade match {
      case "A" => println("Excellent...")
      case "B" => println("Good...")
      case "C" => println("Just so so...")
      case _ if(name == "lisi") => println(name + ", you are a good boy, but...")
      case _  => println(name + ", you need to work harder...")
    }
  }

  judgeGrade("zhangsan", "D")
  judgeGrade("lisi", "D")


  println
  println("***** 1.3 match Array")
  def greeting(array:Array[String]): Unit = {
    array match {
      case Array("zhangsan") => println("Hi:zhangsan")
      case Array(x,y) => println("Hi:" + x + " , " + y)
      case Array("zhangsan", _*) => println("Hi:zhangsan and other friends...")
      case _ => println("Hi: everybody...")
    }
  }

  greeting(Array("lisi","zhangsan","wangwu"))

  println
  println("***** 1.4 match List")
  def greeting(list:List[String]): Unit = {
    list match {
      case "zhangsan"::Nil => println("Hi:zhangsan")
      case x::y::Nil => println("Hi:" + x + " , " + y)
      case "zhangsan"::tail => println("Hi: zhangsan and other friends...")
      case _ => println("Hi: everybody....")
    }
  }

  greeting(List("PK","zhangsan","lisi"))

  println
  println("***** 1.5 match Type")
  def matchType(obj:Any): Unit = {
    obj match {
      case x:Int => println("Type is Int")
      case s:String => println("Type is String")
      case m:Map[_,_] => {println("Type is Map"); m.foreach(println)}
      case _ => println("Type is other type")
    }
  }

  matchType(123)
  matchType(Map("name"->"PK"))

  println
  println("***** 2 case class")
  def caseClassMatch(person:Person): Unit = {
    person match {
      case CTO(name,floor) => println("CTO name is: " + name + " , floor is: " + floor)
      case Employee(name,floor) => println("Employee name is: " + name + " , floor is: " + floor)
      case Other(name) => println(name + ": does not recognize this person")
    }
  }

  class Person
  case class CTO(name:String, floor:String) extends Person
  case class Employee(name:String, floor:String) extends Person
  case class Other(name:String) extends Person

  caseClassMatch(CTO("Alice", "22"))
  caseClassMatch(Employee("Bob", "2"))
  caseClassMatch(Other("Charlie"))

  println
  println("***** 3. 在 get map 元素中的实际应用")
  // 定义了一个 map : [name - grade]
  val grades = Map("Alice"->"A", "Bob"-> "C")
  // 现在我要根据 name 拿到对应的 grade
  def getGrade(name:String): Unit = {
    val grade = grades.get(name)
    // 使用 match 对 edge case 进行处理
    grade match {
      case Some(grade) => println(name + ": your grade is " + grade)
      case None => println(name + ": no records...")
    }
  }

  getGrade("Alice")
  getGrade("Bob")
  getGrade("Charlie")

}
