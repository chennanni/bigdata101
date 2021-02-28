package max.learn.scala.io

import java.sql.{Connection, DriverManager}

object DBApp {

  def main(args: Array[String]): Unit = {

    val url = "jdbc:mysql://localhost:3306/mysql"
    val username = "root"
    val password = "root"

    var connection:Connection = null
    try{

      // make the connection
      classOf[com.mysql.jdbc.Driver]

      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select host,user from user")
      while(resultSet.next()){
        val host = resultSet.getString("host")
        val user = resultSet.getString("user")

        println(s"$host, $user")
      }
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      // free
      if(connection == null) {
        connection.close()
      }
    }
  }

}
