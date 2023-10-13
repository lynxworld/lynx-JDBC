package org.grapheco.db

import com.typesafe.scalalogging.LazyLogging

import java.sql.{Connection, DriverManager, ResultSet}

class DB(val url: String, val username: String, val password: String) extends LazyLogging{
  val driver = "com.mysql.cj.jdbc.Driver"
  Class.forName(driver)

  def connection: Connection = DriverManager.getConnection(url, username, password)

}
