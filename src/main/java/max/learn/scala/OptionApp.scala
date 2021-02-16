package max.learn.scala

object OptionApp extends App {

  val m = Map(1 -> 1)
  println("***** normal get")
  println(m(1))

  println("***** option get")
  println(m.get(1).get)

  println("***** option get: not found")
  println(m.getOrElse(2, "None"))
  //println(m(2)) // java.util.NoSuchElementException: key not found: 2
  //println(m.get(2).get) // java.util.NoSuchElementException: None.get

}

/**
  * case object None extends Option[Nothing] {
      def isEmpty = true
      def get = throw new NoSuchElementException("None.get")
    }

    final case class Some[+A](x: A) extends Option[A] {
      def isEmpty = false
      def get = x
    }
  */
