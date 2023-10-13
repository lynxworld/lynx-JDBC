package org.grapheco.schema


import java.sql.Connection


object ToSchema {
  case class RelMap(source: Seq[String], target: Seq[String])

  var vertex_Schema = Map.empty[String, Array[(String, String)]]
  var rel_Schema = Map.empty[String, Array[(String, String)]]
  var rel_Mapping: Map[String, RelMap] = Map.empty

  def init(connection: Connection): Unit = {

    if (connection != null) {
      val databaseMetaData = connection.getMetaData

      val catalog = connection.getCatalog
      val schemaPattern = null
      val tableNamePattern = null


      val tablesResultSet = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, null)

      while (tablesResultSet.next()) {

        val tableName = tablesResultSet.getString("TABLE_NAME")
        val columnsResultSet = databaseMetaData.getColumns(null, null, tableName, null)
        var referencesCount = 0
        val exportedKeysResultSet = databaseMetaData.getExportedKeys(null, null, tableName)
        while (exportedKeysResultSet.next()) {
          referencesCount += 1
        }

        var fkCount = 0
        var fkArray = Array.empty[(String, String, String)]
        val foreignKeyResultSet = databaseMetaData.getImportedKeys(catalog, schemaPattern, tableName)
        while (foreignKeyResultSet.next()) {
          fkCount += 1
          val fkColumnName = foreignKeyResultSet.getString("FKCOLUMN_NAME")
          val pkTableName = foreignKeyResultSet.getString("PKTABLE_NAME")
          val pkColumnName = foreignKeyResultSet.getString("PKCOLUMN_NAME")
          fkArray = fkArray :+ (fkColumnName, pkTableName, pkColumnName)
        }
        var pkCount = 0
        val primaryKeyResultSet = databaseMetaData.getPrimaryKeys(catalog, schemaPattern, tableName)
        var pkColumnName = ""
        while (primaryKeyResultSet.next()) {
          pkCount += 1
          pkColumnName = primaryKeyResultSet.getString("COLUMN_NAME")
        }
        var array = Array.empty[(String, String)]
        var seq: Seq[String] = Seq()
        while (columnsResultSet.next()) {
          val columnName = columnsResultSet.getString("COLUMN_NAME")
          val columnType = columnsResultSet.getString("TYPE_NAME")
          if (!columnName.contains(":")) array = array :+ (columnName, columnType)
          seq = seq :+ columnName
        }
        (fkCount, pkCount) match {
          case (0, 1) => vertex_Schema = vertex_Schema ++ Map(tableName -> array)
          case (p, 1) if p > 0 =>
            vertex_Schema = vertex_Schema ++ Map(tableName -> array)
            rel_Schema = rel_Schema ++ Map(tableName -> array)
            addRelMap()
          case _ => rel_Schema = rel_Schema ++ Map(tableName -> array)
        }

        def addRelMap(): Unit = {
          if (pkCount == 1) {
            rel_Mapping = rel_Mapping ++ Map(tableName -> RelMap(Array(tableName), Array(fkArray(0)._2)))
          }
          for (i <- 0 until fkCount - 1) {
            rel_Mapping = rel_Mapping ++ Map(tableName -> RelMap(Array(fkArray(i)._2, fkArray(i + 1)._2), Array(fkArray(i)._2, fkArray(i + 1)._2)))
          }
        }
      }
    }
    //    (vertex_Schema, rel_Schema, rel_Mapping)
  }

  lazy val nodeSchema = vertex_Schema
  lazy val relSchema = rel_Schema
  lazy val relMapping = rel_Mapping
}

