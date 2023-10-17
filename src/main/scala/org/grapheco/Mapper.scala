package org.grapheco

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.schema.{Schema, SchemaManager}

import java.sql.ResultSet
import java.time.LocalDate

object Mapper {

  private def mapId(row: ResultSet, colName: String): LynxIntegerID = LynxIntegerID(row.getLong(colName))

  private def mapRelId(row: ResultSet, colName: String): LynxIntegerID = LynxIntegerID(row.getLong(colName))

  private def mapStartId(row: ResultSet, colName: String): LynxIntegerID = LynxIntegerID(row.getLong(colName))

  private def mapEndId(row: ResultSet, colName: String): LynxIntegerID = LynxIntegerID(row.getLong(colName))

  private def mapProps(row: ResultSet, mapper: Array[(String, String)]): Map[LynxPropertyKey, LynxValue] = {
    mapper.map { case (col, typo) =>
      val i = row.findColumn(col)
      LynxPropertyKey(col) -> LynxValue(typo match {
        case "String" => row.getString(i)
        case "BIGINT" => row.getLong(i)
        case "INT" => row.getInt(i)
        case "Date" => LocalDate.ofEpochDay(row.getDate(i).getTime / 86400000)
        case _ => row.getString(i)
      })
    }.toMap
  }


  def mapNode(row: ResultSet, tableName: String, schema: Schema): LynxJDBCNode = {
    val (colName, nodeProps) = SchemaManager.findNodeByName(schema, tableName).map(node => (node.id, node.properties)).get
    LynxJDBCNode(mapId(row, colName), Seq(LynxNodeLabel(tableName)), mapProps(row, nodeProps))
  }


  def mapRel(row: ResultSet, tableName: String, schema: Schema): LynxJDBCRelationship = {
    val (colName, srcColName, dstColName, props) =
      SchemaManager.findRelByName(schema, tableName).map(node => (node.table_id, node.f1_name, node.f2_name, node.properties)).get
    LynxJDBCRelationship(mapRelId(row, colName), mapStartId(row, srcColName), mapEndId(row, dstColName), Some(LynxRelationshipType(tableName)), mapProps(row, props))
  }
}
