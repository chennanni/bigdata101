package max.learn.scala.io

import java.io.{FileInputStream, InputStreamReader}

import scala.xml.XML

object XMLApp {

  def main(args: Array[String]): Unit = {

    loadXML()

    readXMLAttr()

    updateXML()

  }

  def loadXML(): Unit = {
    println("***** 1.1 load xml 1")
    val xml_1 = XML.load(new FileInputStream("D:\\codebase\\bigdataez\\data\\scala\\books.xml"))
    println(xml_1)

    println
    println("***** 1.2 load xml 2")
    val xml_2 = XML.load(
      new InputStreamReader(
        new FileInputStream("D:\\codebase\\bigdataez\\data\\scala\\books.xml")
      )
    )
    println(xml_2)

    //val xml_3 = XML.load(this.getClass.getClassLoader.getResource("books.xml"))
  }

  def readXMLAttr(): Unit = {
    val xml = XML.load(new FileInputStream("D:\\codebase\\bigdataez\\data\\scala\\html.xml"))

    println("***** 2.1 header/field")
    val headerField = xml \ "header" \ "field"
    println(headerField)

    println
    println("***** 2.2 header/field/name")
    // val fieldAttributes = (xml \ "header" \ "field").map(_ \ "@name")
    val fieldAttributes = (xml \ "header" \ "field" \\ "@name")
    for (fieldAttribute <- fieldAttributes) {
      println(fieldAttribute)
    }

    println
    println("***** 2.3 all field")
    val fields = xml \\ "field"
    for (field <- fields) {
      println(field)
    }

    println
    println("***** 2.4 name = Logon message")
    // val filters = (xml \\ "message").filter(_.attribute("name").exists(_.text.equals("Logon")))
    val filters = (xml \\ "message").filter(x => ((x \ "@name").text).equals("Logon"))
    for (filter <- filters) {
      println(filter)
    }

    println
    println("***** 2.5 header/field  content")
    (xml \ "header" \ "field")
      .map(x => (x \ "@name", x.text, x \ "@required"))
      .foreach(println)

  }

  def updateXML(): Unit ={
    // read xml
    val xml = XML.load(new FileInputStream("D:\\codebase\\bigdataez\\data\\scala\\books.xml"))
    val bookMap = scala.collection.mutable.HashMap[String,String]()

    // load some data
    (xml \ "book").map(x => {
      val id = (x \ "@id").toString()
      val name = (x \ "name").text.toString
      bookMap(id) = name
    })

    // new xml
    val newXml = <bookshelf>{bookMap.map(updateXmlFile)}</bookshelf>
    XML.save("D:\\codebase\\bigdataez\\data\\scala\\newBooks.xml", newXml)
    println
    println("***** 3. new xml")
    println(newXml)

  }

  def updateXmlFile(ele:(String,String)) = {
    val (id, oldName) = ele

    <book id={id + "-new"}>
      <name>{oldName + " Programming"}</name>
    </book>
  }

}
