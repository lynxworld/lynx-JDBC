
package org.example

import java.sql.{Connection, DriverManager}


case class TableDefinition(
      table_name: String, // 数据库表名
      id: String, // 主键列名
      label: String, // label列名
      properties: List[String] // 其他列名
                          )

object ToSchema {
  def main(args: Array[String]): Unit = {
    // 设置数据库连接信息
    val url = "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "Hc1478963!"
    Class.forName(driver)

    val connection: Connection = DriverManager.getConnection(url, username, password)


    if (connection != null) {
      //
      val databaseMetaData = connection.getMetaData

      //
      val catalog = connection.getCatalog
      val schemaPattern = null
      val tableNamePattern = null


      val tablesResultSet = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, null)
      while (tablesResultSet.next()) {


        val tableName = tablesResultSet.getString("TABLE_NAME")


        //
        val columnsResultSet = databaseMetaData.getColumns(null, null, tableName, null)
        while (columnsResultSet.next()) {
          val columnName = columnsResultSet.getString("COLUMN_NAME")
          val columnType = columnsResultSet.getString("TYPE_NAME")
          println(s"Column Name: $columnName :  Type: $columnType")
        }
//
//        val primaryKeyResultSet = databaseMetaData.getPrimaryKeys(catalog, schemaPattern, tableName)
//        while (columnsResultSet.next()) {
//          val pkColumnName = primaryKeyResultSet.getString("COLUMN_NAME")
//          println(s"Vertex Table : $tableName")
//          println(s"Primary Key Column: $pkColumnName")
//        }
//        val tableGraph = TableDefinition(
//          table_name = columnsResultSet.getString("COLUMN_NAME"),
//          id = primaryKeyResultSet.getString("COLUMN_NAME"),
//          label = ???,
//          properties = ???
//        )
        //


        //
        val foreignKeyResultSet = databaseMetaData.getImportedKeys(catalog, schemaPattern, tableName)
        while (foreignKeyResultSet.next()) {
          val fkColumnName = foreignKeyResultSet.getString("FKCOLUMN_NAME")
          val pkTableName = foreignKeyResultSet.getString("PKTABLE_NAME")
          val pkColumnName = foreignKeyResultSet.getString("PKCOLUMN_NAME")
          println(s"Foreign Key Column: $fkColumnName, Referenced Table: $pkTableName, Referenced Column: $pkColumnName")
        }

        val exportedKeysResultSet = databaseMetaData.getExportedKeys(null, null, tableName)
        while (exportedKeysResultSet.next()) {
          val fkColumnName = exportedKeysResultSet.getString("FKCOLUMN_NAME")
          val pkTableName = exportedKeysResultSet.getString("PKTABLE_NAME")
          val pkColumnName = exportedKeysResultSet.getString("PKCOLUMN_NAME")
          println(s"Foreign Key Column: $fkColumnName, Referenced Table: $pkTableName, Referenced Column: $pkColumnName")
        }
      }
    }

  }
}
