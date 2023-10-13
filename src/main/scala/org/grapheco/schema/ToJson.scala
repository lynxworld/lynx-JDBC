package org.grapheco.schema

import play.api.libs.json.{Json, Writes}

import java.io.PrintWriter
import java.sql.{DatabaseMetaData, DriverManager}

case class VertexTableStructure(
                                 table_name: String,
                                 prefixed_id: Boolean,
                                 id: String,
                                 fix_label: Boolean,
                                 label: String,
                                 properties: Seq[String]
                               )

object VertexTableStructure {
  implicit val dataWrites: Writes[VertexTableStructure] = Json.writes[VertexTableStructure]
}

case class EdgeTableStructure(
                               table_name: String,
                               src_v_table: String,
                               src_v: String,
                               dst_v_table: String,
                               dst_v: String,
                               prefixed_edge_id: Boolean,
                               id: String,
                               label: String
                             )

object EdgeTableStructure {
  implicit val dataWrites: Writes[EdgeTableStructure] = Json.writes[EdgeTableStructure]
}

object ToJson {

  def main(args: Array[String]): Unit = {
    val config = loadConfiguration()
    val (vertexTables, edgeTables) = processDatabaseTables(config)
    writeSchemaDataToJson(vertexTables, edgeTables)
  }

  private case class DatabaseConfig(url: String, username: String, password: String)

  private def loadConfiguration(): DatabaseConfig = {
    //    val url = "jdbc:mysql://localhost:3306/health_care?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false"
    //    val password = "root"
    val url = "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false"
    val password = "Hc1478963!"
    val username = "root"

    DatabaseConfig(url, username, password)
  }

  private def processDatabaseTables(config: DatabaseConfig): (Seq[VertexTableStructure], Seq[EdgeTableStructure]) = {
    var vertexTables = Seq[VertexTableStructure]()
    var edgeTables = Seq[EdgeTableStructure]()

    using(DriverManager.getConnection(config.url, config.username, config.password)) { connection =>
      val databaseMetaData = connection.getMetaData
      val catalog = connection.getCatalog
      val schemaPattern: Null = null
      val tableNamePattern: Null = null

      val tablesResultSet = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, null)

      while (tablesResultSet.next()) {
        val tableName = tablesResultSet.getString("TABLE_NAME")
        val (fkCount, pkCount, pkColumnName, pkArray, seq) = processTable(databaseMetaData, catalog, tableName)
        (fkCount, pkCount) match {
          case (0, 1) => vertexTables = vertexTables :+ createVertexTable(tableName, pkColumnName, seq)
          case (p, 1) if p > 0 =>
            vertexTables = vertexTables :+ createVertexTable(tableName, pkColumnName, seq)
            edgeTables = edgeTables ++ createEdgeTable(tableName, pkCount, pkColumnName, pkArray, fkCount)
          case _ => edgeTables = edgeTables ++ createEdgeTable(tableName, pkCount, pkColumnName, pkArray, fkCount)
        }
      }
    }
    (vertexTables, edgeTables)
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


  private def createVertexTable(tableName: String, pkColumnName: String, seq: Seq[String]): VertexTableStructure = {
    VertexTableStructure(
      table_name = tableName,
      prefixed_id = true,
      id = pkColumnName,
      fix_label = true,
      label = tableName,
      properties = seq
    )
  }

  private def createEdgeTable(tableName: String, pkCount: Int, pkColumnName: String, pkArray: Array[(String, String, String)], fkCount: Int): Seq[EdgeTableStructure] = {
    var tables = Seq[EdgeTableStructure]()
    if (pkCount == 1) {
      tables = tables :+ EdgeTableStructure(
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
      tables = tables :+ EdgeTableStructure(
        table_name = tableName,
        src_v_table = pkArray(i + 1)._2,
        src_v = pkArray(i + 1)._3,
        dst_v_table = pkArray(i)._2,
        dst_v = pkArray(i)._3,
        prefixed_edge_id = true,
        id = s"'$tableName'::${pkArray(i + 1)._1}::${pkArray(i)._1}",
        label = tableName
      )
      tables = tables :+ EdgeTableStructure(
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

  private def writeSchemaDataToJson(vertexTables: Seq[VertexTableStructure], edgeTables: Seq[EdgeTableStructure]): Unit = {
    val vertexJsonString = Json.toJson(vertexTables).toString()
    val edgeJsonString = Json.toJson(edgeTables).toString()
    val vertexTablesPath = "vertex_Tables.json"
    val edgeTablesPath = "edge_Tables.json"

    val v_writer = new PrintWriter(vertexTablesPath)
    v_writer.write(vertexJsonString)
    v_writer.close()
    val e_writer = new PrintWriter(edgeTablesPath)
    e_writer.write(edgeJsonString)
    e_writer.close()
  }

  private def using[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    var t: Throwable = null
    try {
      block(resource)
    } catch {
      case x: Throwable => t = x; throw x
    } finally {
      if (t == null) {
        if (resource != null) resource.close()
      } else {
        if (resource != null) resource.close()
      }
    }
  }
}
