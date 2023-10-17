package org.grapheco

import org.grapheco.db.DB
import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.runner.{CypherRunner, GraphModel}
import org.grapheco.schema.{Schema, SchemaManager}

object LynxJDBCConnector {
  def connect(url: String, username: String, password: String): LynxJDBCConnector = {
    val db: DB = new DB(url, username, password)
    val schema: Schema = SchemaManager.autoGeneration(db.connection)
    val graphModel: JDBCGraphModel = new JDBCGraphModel(db.connection, schema)
    val runner = new CypherRunner(graphModel)

    new LynxJDBCConnector(graphModel, runner, schema)
  }
}

case class LynxJDBCConnector(graphModel: GraphModel, runner: CypherRunner, schema: Schema) {

  def run(query: String, param: Map[String, Any] = Map.empty): LynxResult = runner.run(query, param)

}

