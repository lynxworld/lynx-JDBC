package org.grapheco.schema

import play.api.libs.json.{Json, Writes}

case class NodeTableStructure(
                                 table_name: String,
                                 prefixed_id: Boolean,
                                 id: String,
                                 fix_label: Boolean,
                                 label: String,
                                 properties: Seq[String]
                               )

object NodeTableStructure {
  implicit val dataWrites: Writes[NodeTableStructure] = Json.writes[NodeTableStructure]
}
