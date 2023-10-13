package org.grapheco.schema

import play.api.libs.json.Json
import schema.RelationshipTableStructure

import java.io.PrintWriter
import java.sql.{Connection, DatabaseMetaData}

case class ToJson(connection: Connection) {
  def saveJson(): Unit = {
    val (nodeTables, relationshipTables) = processDatabaseTables(connection)
    writeSchemaDataToJson(nodeTables, relationshipTables)
  }

  private def processDatabaseTables(connection: Connection): (Seq[NodeTableStructure], Seq[RelationshipTableStructure]) = {
    var nodeTables = Seq[NodeTableStructure]()
    var relationshipTables = Seq[RelationshipTableStructure]()
    //    val connection: Connection = DB.connection
    val databaseMetaData = connection.getMetaData
    val catalog = connection.getCatalog
    val schemaPattern: Null = null
    val tableNamePattern: Null = null

    val tablesResultSet = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, null)

    while (tablesResultSet.next()) {
      val tableName = tablesResultSet.getString("TABLE_NAME")
      val (fkCount, pkCount, pkColumnName, pkArray, seq) = processTable(databaseMetaData, catalog, tableName)
      (fkCount, pkCount) match {
        case (0, 1) => nodeTables = nodeTables :+ createNodeTable(tableName, pkColumnName, seq)
        case (p, 1) if p > 0 =>
          nodeTables = nodeTables :+ createNodeTable(tableName, pkColumnName, seq)
          relationshipTables = relationshipTables ++ createRelTable(tableName, pkCount, pkColumnName, pkArray, fkCount)
        case _ => relationshipTables = relationshipTables ++ createRelTable(tableName, pkCount, pkColumnName, pkArray, fkCount)
      }
    }
    //    }
    (nodeTables, relationshipTables)
  }

  private def processTable(databaseMetaData: DatabaseMetaData, catalog: String, tableName: String): (Int, Int, String, Array[(String, String, String)], Seq[String]) = {
    var fkCount = 0
    var pkCount = 0
    var pkColumnName = ""
    var pkArray = Array.empty[(String, String, String)]
    var columnNames = Seq.empty[String]

    val foreignKeyResultSet = databaseMetaData.getImportedKeys(catalog, null, tableName)
    while (foreignKeyResultSet.next()) {
      fkCount += 1
      val fkColumnName = foreignKeyResultSet.getString("FKCOLUMN_NAME")
      val pkTableName = foreignKeyResultSet.getString("PKTABLE_NAME")
      val pkColumnName = foreignKeyResultSet.getString("PKCOLUMN_NAME")
      pkArray = pkArray :+ (fkColumnName, pkTableName, pkColumnName)
    }

    val primaryKeyResultSet = databaseMetaData.getPrimaryKeys(catalog, null, tableName)
    while (primaryKeyResultSet.next()) {
      pkCount += 1
      pkColumnName = primaryKeyResultSet.getString("COLUMN_NAME")
    }

    val columnsResultSet = databaseMetaData.getColumns(catalog, null, tableName, null)
    while (columnsResultSet.next()) {
      val columnName = columnsResultSet.getString("COLUMN_NAME")
      columnNames = columnNames :+ columnName
    }

    (fkCount, pkCount, pkColumnName, pkArray, columnNames)
  }

  private def createNodeTable(tableName: String, pkColumnName: String, seq: Seq[String]): NodeTableStructure = {
    NodeTableStructure(
      table_name = tableName,
      prefixed_id = true,
      id = pkColumnName,
      fix_label = true,
      label = tableName,
      properties = seq
    )
  }

  private def createRelTable(tableName: String, pkCount: Int, pkColumnName: String, pkArray: Array[(String, String, String)], fkCount: Int): Seq[RelationshipTableStructure] = {
    var tables = Seq[RelationshipTableStructure]()
    if (pkCount == 1) {
      tables = tables :+ RelationshipTableStructure(
        table_name = tableName,
        src_v_table = tableName,
        src_v = pkColumnName,
        dst_v_table = pkArray(0)._2,
        dst_v = pkArray(0)._3,
        prefixed_edge_id = true,
        id = s"'$tableName'::$pkColumnName::${pkArray(0)._1}",
        label = tableName
      )
    }
    for (i <- 0 until fkCount - 1) {
      tables = tables :+ RelationshipTableStructure(
        table_name = tableName,
        src_v_table = pkArray(i + 1)._2,
        src_v = pkArray(i + 1)._3,
        dst_v_table = pkArray(i)._2,
        dst_v = pkArray(i)._3,
        prefixed_edge_id = true,
        id = s"'$tableName'::${pkArray(i + 1)._1}::${pkArray(i)._1}",
        label = tableName
      )
      tables = tables :+ RelationshipTableStructure(
        table_name = tableName,
        src_v_table = pkArray(i)._2,
        src_v = pkArray(i)._3,
        dst_v_table = pkArray(i + 1)._2,
        dst_v = pkArray(i + 1)._3,
        prefixed_edge_id = true,
        id = s"'$tableName'::${pkArray(i)._1}::${pkArray(i + 1)._1}",
        label = tableName
      )
    }
    tables
  }

  private def writeSchemaDataToJson(nodeTables: Seq[NodeTableStructure], relationshipTables: Seq[RelationshipTableStructure]): Unit = {
    val jsonContext = Json.prettyPrint(
      Json.obj(
        "Node_Tables" -> Json.toJson(nodeTables),
        "Relationship_Tables" -> Json.toJson(relationshipTables)))
    val jsonPath = "Schema.json"
    val v_writer = new PrintWriter(jsonPath)
    v_writer.write(jsonContext)
    v_writer.close()
  }
}
