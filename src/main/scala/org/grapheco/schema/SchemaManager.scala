package org.grapheco.schema

import org.grapheco.schema.NodeStructure.nodeWrites
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json.{JsValue, Json}

import java.io.PrintWriter
import java.sql.{Connection, DatabaseMetaData}
import scala.io.Source

object SchemaManager {

  def readJson(filePath: String): Schema = {
    val jsonPath = if (filePath.isEmpty) "Schema.json" else s"$filePath"

    try {
      val jsonString = Source.fromFile(jsonPath).mkString
      val json = Json.parse(jsonString)

      val nodeSchema = (json \ "Node_Tables").as[JsValue].asOpt[Seq[NodeStructure]].get
      val relSchema = (json \ "Relationship_Tables").as[JsValue].asOpt[Seq[RelStructure]].get
      val relMap = (json \ "Relationship_Tables").as[JsValue].asOpt[Map[String, RelMap]].get

      Schema(nodeSchema, relSchema, relMap)
    } catch {
      case e: Exception =>
        println(s"Error reading JSON from file: ${e.getMessage}")
        null
    }
  }

  def update(): Unit = {
    //TODO
  }

  def saveJson(filePath: String, schema: Schema): Unit = {
    writeSchemaDataToJson(filePath, schema)
  }

  def findRelByName(schema: Schema, tableName: String): Option[RelStructure] = schema.relSchema.find(_.table_name == tableName)

  def findNodeByLabel(schema: Schema, label: String): Option[NodeStructure] = schema.nodeSchema.find(_.label == label)
  def findRelByLabel(schema: Schema, label: String): Option[RelStructure] = schema.relSchema.find(_.label == label)
  def findNodeByName(schema: Schema, tableName: String): Option[NodeStructure] = schema.nodeSchema.find(_.table_name == tableName)

  def autoGeneration(connection: Connection): Schema = {
    var nodeTables = Seq[NodeStructure]()
    var relationshipTables = Seq[RelStructure]()
    var rel_Mapping: Map[String, RelMap] = Map.empty
    val databaseMetaData = connection.getMetaData
    val catalog = connection.getCatalog
    val schemaPattern: Null = null
    val tableNamePattern: Null = null

    val tablesResultSet = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, null)

    while (tablesResultSet.next()) {
      val tableName = tablesResultSet.getString("TABLE_NAME")
      val (fkCount, pkCount, pkColumnName, fkArray, arr) = processTable(databaseMetaData, catalog, tableName)
      (fkCount, pkCount) match {
        case (0, 1) => nodeTables = nodeTables :+ createNodeTable(tableName, pkColumnName, arr)
        case (p, 1) if p > 0 =>
          nodeTables = nodeTables :+ createNodeTable(tableName, pkColumnName, arr)
          relationshipTables = relationshipTables ++ createRelTable(tableName, pkCount, pkColumnName, fkArray, fkCount, arr)
          rel_Mapping = rel_Mapping ++ Map(tableName -> RelMap(Array(tableName), Array(fkArray(0)._2)))
          for (i <- 0 until fkCount - 1) {
            rel_Mapping = rel_Mapping ++ Map(tableName -> RelMap(Array(fkArray(i)._2, fkArray(i + 1)._2), Array(fkArray(i)._2, fkArray(i + 1)._2)))
          }
        case _ => relationshipTables = relationshipTables ++ createRelTable(tableName, pkCount, pkColumnName, fkArray, fkCount, arr)
          for (i <- 0 until fkCount - 1) {
            rel_Mapping = rel_Mapping ++ Map(tableName -> RelMap(Array(fkArray(i)._2, fkArray(i + 1)._2), Array(fkArray(i)._2, fkArray(i + 1)._2)))
          }
      }

    }
    Schema(nodeTables, relationshipTables, rel_Mapping)
  }

  private def processTable(databaseMetaData: DatabaseMetaData, catalog: String, tableName: String): (Int, Int, String, Array[(String, String, String)], Array[(String, String)]) = {
    var fkCount = 0
    var pkCount = 0
    var pkColumnName = ""
    var fkArray = Array.empty[(String, String, String)]
    var propertyArray = Array.empty[(String, String)]
    var seq: Seq[String] = Seq()

    val foreignKeyResultSet = databaseMetaData.getImportedKeys(catalog, null, tableName)
    while (foreignKeyResultSet.next()) {
      fkCount += 1
      val fkColumnName = foreignKeyResultSet.getString("FKCOLUMN_NAME")
      val pkTableName = foreignKeyResultSet.getString("PKTABLE_NAME")
      val pkColumnName = foreignKeyResultSet.getString("PKCOLUMN_NAME")
      fkArray = fkArray :+ (fkColumnName, pkTableName, pkColumnName)
    }

    val primaryKeyResultSet = databaseMetaData.getPrimaryKeys(catalog, null, tableName)
    while (primaryKeyResultSet.next()) {
      pkCount += 1
      pkColumnName = primaryKeyResultSet.getString("COLUMN_NAME")
    }

    val columnsResultSet = databaseMetaData.getColumns(catalog, null, tableName, null)
    while (columnsResultSet.next()) {
      val columnName = columnsResultSet.getString("COLUMN_NAME")
      val columnType = columnsResultSet.getString("TYPE_NAME")
      if (!columnName.contains(":")) propertyArray = propertyArray :+ (columnName, columnType)
    }

    (fkCount, pkCount, pkColumnName, fkArray, propertyArray)
  }

  private def createNodeTable(tableName: String, pkColumnName: String, arr: Array[(String, String)]): NodeStructure = {
    NodeStructure(
      table_name = tableName,
      prefixed_id = true,
      id = pkColumnName,
      fix_label = true,
      label = tableName,
      properties = arr
    )
  }

  private def createRelTable(tableName: String, pkCount: Int, pkColumnName: String, pkArray: Array[(String, String, String)], fkCount: Int, arr: Array[(String, String)]): Seq[RelStructure] = {
    var tables = Seq[RelStructure]()
    for (i <- 0 until fkCount - 1) {
      tables = tables :+ RelStructure(
        table_name = tableName,
        table_id = pkColumnName,
        src_v_table = pkArray(i + 1)._2,
        src_v = pkArray(i + 1)._3,
        f1_name = pkArray(i + 1)._1,
        dst_v_table = pkArray(i)._2,
        dst_v = pkArray(i)._3,
        f2_name = pkArray(i)._1,
        prefixed_edge_id = true,
        id = s"'$tableName'::${pkArray(i + 1)._1}::${pkArray(i)._1}",
        label = tableName,
        properties = arr
      )
      tables = tables :+ RelStructure(
        table_name = tableName,
        table_id = pkColumnName,
        src_v_table = pkArray(i)._2,
        src_v = pkArray(i)._3,
        f1_name = pkArray(i)._1,
        dst_v_table = pkArray(i + 1)._2,
        dst_v = pkArray(i + 1)._3,
        f2_name = pkArray(i + 1)._1,
        prefixed_edge_id = true,
        id = s"'$tableName'::${pkArray(i)._1}::${pkArray(i + 1)._1}",
        label = tableName,
        properties = arr
      )
    }
    if (pkCount == 1) {
      tables = tables :+ RelStructure(
        table_name = tableName,
        table_id = pkColumnName,
        src_v_table = tableName,
        src_v = pkColumnName,
        f1_name = pkColumnName,
        dst_v_table = pkArray(0)._2,
        dst_v = pkArray(0)._3,
        f2_name = pkArray(0)._1,
        prefixed_edge_id = true,
        id = s"'$tableName'::$pkColumnName::${pkArray(0)._1}",
        label = tableName,
        properties = arr,

      )
    }
    tables
  }

  private def writeSchemaDataToJson(filePath: String, schema: Schema): Unit = {
    val jsonContext = Json.prettyPrint(
      Json.obj(
        "Node_Tables" -> Json.toJson(schema.nodeSchema),
        "Relationship_Tables" -> Json.toJson(schema.relSchema),
        "Relationship_Map" -> Json.toJson(schema.relMapping)))

    val jsonPath = filePath match {
      case "" => "Schema.json"
      case _ => filePath
    }
    "Schema.json"
    val v_writer = new PrintWriter(jsonPath)
    v_writer.write(jsonContext)
    v_writer.close()
  }
}
