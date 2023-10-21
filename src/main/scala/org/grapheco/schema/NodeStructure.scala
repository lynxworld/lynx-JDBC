package org.grapheco.schema


import play.api.libs.json.{Json, Reads, Writes}

case class NodeStructure(table_name: String, prefixed_id: Boolean, id: String, fix_label: Boolean, label: String, properties: Array[(String, String)])

object NodeStructure {
  implicit val nodeWrites: Writes[NodeStructure] = Json.writes[NodeStructure]
  implicit val nodeReads: Reads[NodeStructure] = Json.reads[NodeStructure]
}