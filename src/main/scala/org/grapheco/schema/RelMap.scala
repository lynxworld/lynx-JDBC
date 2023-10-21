package org.grapheco.schema
import play.api.libs.json.{Json, Reads, Writes}

case class RelMap(source: Seq[String], target: Seq[String])

object RelMap {
  implicit val relWrites: Writes[RelMap] = Json.writes[RelMap]
  implicit val relReads: Reads[RelMap] = Json.reads[RelMap]
}