package org.grapheco

import org.grapheco.db.DB
import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.runner.{CypherRunner, GraphModel}

object LynxJDBCConnector {
  def connect(url: String, username: String, password: String): LynxJDBCConnector = {
    val db: DB = new DB(url, username, password)
    val graphModel: JDBCGraphModel = new JDBCGraphModel(db.connection)
    val runner = new CypherRunner(graphModel)

    new LynxJDBCConnector(graphModel, runner)
  }
}

case class LynxJDBCConnector(graphModel: GraphModel, runner: CypherRunner) {

  def run(query: String, param: Map[String, Any] = Map.empty): LynxResult = runner.run(query, param)
}
