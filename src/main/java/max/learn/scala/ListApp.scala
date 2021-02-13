package max.learn.scala

object ListApp extends App{

  println("***** 1.1 list 1")
  val l1 = List(1,2,3,4,5)
  println("list 1: " + l1)
  println("list 1 header: " + l1.head)
  println("list 1 tail: " + l1.tail)

  println("***** 1.2 list 2")
  val l2 = 1 :: 2 :: Nil
  println("list 2: " + l2)

  println("***** 1.3 list 3")
  val l3 = l2 :: 3 :: Nil
  println("list 3: " + l3)

  println("***** 1.4 list 4")
  val l4 = 1 :: 2 :: 3 :: 4 :: Nil
  println("list 4: " + l4)

  println()
  println("***** 2 可变长度 List")
  val l5 = scala.collection.mutable.ListBuffer[Int]()
  l5 += 2
  l5 += (3,4,5)
  l5 ++= List(6,7,8,9)
  println("list 5: " + l5)

  l5 -= 2
  l5 -= 3
  l5 -= (1, 4)
  l5 --= List(5,6,7,8)
  println("list 5: " + l5)

  println("toList: " + l5.toList)
  println("toArray: " + l5.toArray)
  println("toSet: " + l5.toSet)

  println()
  println("***** 3 递归调用")
  def sum(nums:Int*):Int = {
    if(nums.length == 0) {
      0
    } else {
      nums.head + sum(nums.tail:_*)
    }
  }
  println("sum 0: " + sum())
  println("sum 1-4: " + sum(1,2,3,4))
}
