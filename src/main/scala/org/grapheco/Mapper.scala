package org.grapheco

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.schema.SchemaManager.Schema

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

  def mapNode(row: ResultSet, tableName: String, mapper: Array[(String, String)], colName: String): LynxJDBCNode =
    LynxJDBCNode(mapId(row, colName), Seq(LynxNodeLabel(tableName)), mapProps(row, mapper))

  def mapRel(row: ResultSet, relName: String, colName: (String,String,String), mapper: Array[(String, String)]): LynxJDBCRelationship =
    LynxJDBCRelationship(mapRelId(row, colName._1), mapStartId(row, colName._2), mapEndId(row, colName._3), Some(LynxRelationshipType(relName)), mapProps(row, mapper))

}
