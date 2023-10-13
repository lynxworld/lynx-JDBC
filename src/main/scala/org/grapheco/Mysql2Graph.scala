package org.grapheco

import org.grapheco.db.DB
import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.runner.{CypherRunner, GraphModel, NodeFilter, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._

class Mysql2Graph extends GraphModel {
  val connnect: Any = DB.connection

  val runner = new CypherRunner(this)

  def run(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = runner.run(query, param)

  /**
   * An WriteTask object needs to be returned.
   * There is no default implementation, you must override it.
   *
   * @return The WriteTask object
   */
  override def write: WriteTask = new WriteTask {
    override def createElements[T](nodesInput: Seq[(String, NodeInput)], relationshipsInput: Seq[(String, RelationshipInput)], onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = ???

    override def deleteRelations(ids: Iterator[LynxId]): Unit = ???

    override def deleteNodes(ids: Seq[LynxId]): Unit = ???

    override def updateNode(lynxId: LynxId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]): Option[LynxNode] = ???

    override def updateRelationShip(lynxId: LynxId, props: Map[LynxPropertyKey, LynxValue]): Option[LynxRelationship] = ???

    override def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)], cleanExistProperties: Boolean): Iterator[Option[LynxNode]] = ???

    override def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = ???

    override def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)]): Iterator[Option[LynxRelationship]] = ???

    override def setRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = ???

    override def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxNode]] = ???

    override def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = ???

    override def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxRelationship]] = ???

    override def removeRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = ???

    override def commit: Boolean = true
  }

  /**
   * Find Node By ID.
   *
   * @return An Option of node.
   */
  override def nodeAt(id: LynxId): Option[LynxNode] = ???

  /**
   * All nodes.
   *
   * @return An Iterator of all nodes.
   */
  override def nodes(): Iterator[LynxNode] = ???

  override def nodes(nodeFilter: NodeFilter): Iterator[ElementNode] = {
    println("nodes(nodeFilter)")

    if (nodeFilter.labels.isEmpty && nodeFilter.properties.isEmpty) {
      return nodes()
    }

    val tableName = nodeFilter.labels.head.toString
    val conditions = nodeFilter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray

    var sql = "select * from " + tableName

    // add conditions to the sql query
    for (i <- conditions.indices) {
      if (i == 0) {
        sql = sql + " where " + conditions(i)
      } else {
        sql = sql + " and " + conditions(i)
      }
    }

    println(sql)

    val statement = connnect.createStatement
    val startTime2 = System.currentTimeMillis()
    val data = statement.executeQuery(sql)
    println("nodes(nodeFilter) SQL used: " + (System.currentTimeMillis() - startTime2) + " ms")

    // transform the rows in the sql result to LynxNodes
    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }

    result
  }
  /**
   * Return all relationships as PathTriple.
   *
   * @return An Iterator of PathTriple
   */
  override def relationships(): Iterator[PathTriple] = {
    println("relationships()")

    // iterate all relationship tables
    val allRels = for (tableName <- relSchema.keys) yield {
      val statement = connection.createStatement
      val data = statement.executeQuery(s"select * from $tableName")

      // transform all rows in the table to PathTriples
      Iterator.continually(data).takeWhile(_.next())
        .map { resultSet =>
          val startNode = myNodeAt(MyId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
          val endNode = myNodeAt(MyId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
          val rel = rowToRel(resultSet, tableName, relSchema(tableName))

          PathTriple(startNode, rel, endNode)
        }
    }

    println("relationships() finished")
    allRels.flatten.iterator
  }


  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    println("relationships(relationshipFilter)")

    val tableName = relationshipFilter.types.head.toString
    val conditions = relationshipFilter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray

    var sql = "select * from " + tableName

    // add conditions to the sql query
    for (i <- conditions.indices) {
      if (i == 0) {
        sql = sql + " where " + conditions(i)
      } else {
        sql = sql + " and " + conditions(i)
      }
    }

    println(sql)

    val statement = connection.createStatement
    val startTime1 = System.currentTimeMillis()
    val data = statement.executeQuery(sql)
    println("rel(relFilter) SQL used " + (System.currentTimeMillis() - startTime1) + " ms")

    // transform the rows in the sql result to PathTriples
    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet =>
        val startNode = nodeAt(MyId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
        val endNode = nodeAt(MyId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
        val rel = rowToRel(resultSet, tableName, relSchema(tableName))

        PathTriple(startNode, rel, endNode)
      }

    result
  }
}
