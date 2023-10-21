package org.grapheco.schema


import play.api.libs.json.{Json, Reads, Writes}

case class RelStructure(table_name: String, table_id: String, src_v_table: String, src_v: String, f1_name: String,
                        dst_v_table: String, dst_v: String, f2_name: String, prefixed_edge_id: Boolean,
                        id: String, label: String, properties: Array[(String, String)])

object RelStructure {
  implicit val relWrites: Writes[RelStructure] = Json.writes[RelStructure]
  implicit val relReads: Reads[RelStructure] = Json.reads[RelStructure]
}