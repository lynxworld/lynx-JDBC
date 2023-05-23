package org.example

/**
 * Created by Administrator on 2017/12/23.
 */
import java.sql.{ Connection, DriverManager }


object ScalaJdbcConnectSelect extends App {


  // 访问本地MySQL服务器，通过3306端口访问mysql数据库 "world"
  val url = "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false"
  //驱动名称
  val driver = "com.mysql.cj.jdbc.Driver"

  //用户名
  val username = "root"
  //密码
  val password = "Hc1478963!"
  //初始化数据连接
  var connection: Connection = _
  try {
    //注册Driver
    Class.forName(driver)
    //得到连接
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    //执行查询语句，并返回结果
    val rs = statement.executeQuery("SELECT * FROM hasType")
    val metadata = rs.getMetaData
    val columnCount = metadata.getColumnCount

    //打印返回结果
    while (rs.next) {
      for (i <- 1 to columnCount) {
        val columnName = metadata.getColumnName(i)
        val columnValue = rs.getString(i)
        print("%s = %s  ".format(columnName, columnValue))
      }

      println("\n")
    }

    println("Complete Query operation")

  } catch {
    case e: Exception => e.printStackTrace
  }
  //关闭连接，释放资源
  connection.close
}
