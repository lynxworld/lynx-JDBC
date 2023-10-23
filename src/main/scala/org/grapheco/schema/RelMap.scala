package org.grapheco.schema
import play.api.libs.json.{Json, Reads, Writes}

case class RelMap(source: Seq[String], target: Seq[String])

object RelMap {
  implicit val relMapWrites: Writes[RelMap] = Json.writes[RelMap]
  implicit val relMapReads: Reads[RelMap] = Json.reads[RelMap]
}