package schema

import play.api.libs.json.{Json, Writes}

case class RelationshipTableStructure(
                               table_name: String,
                               src_v_table: String,
                               src_v: String,
                               dst_v_table: String,
                               dst_v: String,
                               prefixed_edge_id: Boolean,
                               id: String,
                               label: String
                             )

object RelationshipTableStructure {
  implicit val dataWrites: Writes[RelationshipTableStructure] = Json.writes[RelationshipTableStructure]
}
