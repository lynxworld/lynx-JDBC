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
    val _schema = SchemaManager.findNodeByName(schema, tableName)
    if (_schema.isEmpty) throw new Exception("schema not find")
    LynxJDBCNode(mapId(row, _schema.get.id), Seq(LynxNodeLabel(_schema.get.label)), mapProps(row, _schema.get.properties))
  }


  def mapRel(row: ResultSet, tableName: String, schema: Schema): LynxJDBCRelationship = {
    val _schema = SchemaManager.findRelByName(schema, tableName)
    if (_schema.isEmpty) throw new Exception("schema not find")
    LynxJDBCRelationship(mapRelId(row, _schema.get.table_id),
      mapStartId(row, _schema.get.f1_name),
      mapEndId(row, _schema.get.f2_name),
      Some(LynxRelationshipType(_schema.get.label)),
      mapProps(row, _schema.get.properties))
  }
}
