package org.grapheco

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.Mapper.{END_ID_COL_NAME, ID_COL_NAME, START_ID_COL_NAME, mapRel}
import org.grapheco.db.DB
import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.runner.{CypherRunner, GraphModel, NodeFilter, RelationshipFilter, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType, PathTriple}
import org.grapheco.schema.ToSchema._
import org.grapheco.schema.ToSchema
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.sql.{Connection, ResultSet}

class JDBCGraphModel(val connection: Connection) extends GraphModel with LazyLogging{
  ToSchema.init(connection)

  private def sql(sql: String): Unit = {
    logger.info(sql)
    val statement = connection.createStatement()
    statement.executeQuery(sql)
  }

  private def iterExecute(sql: String): Iterator[ResultSet] = {
    logger.info(sql)
    val statement = connection.createStatement()
    val result = statement.executeQuery(sql)
    Iterator.continually(result).takeWhile(_.next())
  }

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

  override def nodeAt(id: LynxId): Option[LynxNode] = ???

  private def singleTableSelect(tableName: String, filters: Map[LynxPropertyKey, LynxValue]): Iterator[ResultSet] = {
    val conditions: Map[String, Any] = filters.map { case (k, v) => k.toString -> v.value }
    singleTableSelect(tableName, conditions.toList)
  }

  private def singleTableSelect(tableName: String, conditions: Seq[(String, Any)]): Iterator[ResultSet] = {
    val selectWhat = nodeSchema.getOrElse(tableName, relSchema(tableName)).map(_._1).mkString(",")
    val sql = s"select $selectWhat from ${tableName} ${
      if (conditions.isEmpty) ""
      else " where " + conditions.map { case (key, value) => s"$key = '$value'" }.mkString(" and ")
    }"
    iterExecute(sql)
  }

  private def nodeAt(id: LynxId, tableList: Seq[String]): Option[LynxJDBCNode] = {
    tableList.flatMap { t =>
      singleTableSelect(t, List((ID_COL_NAME, id.toLynxInteger.v)))
        .map(rs => Mapper.mapNode(rs, t, nodeSchema(t))).toSeq.headOption
    }.headOption
  }

  override def nodes(): Iterator[LynxJDBCNode] = nodeSchema.keys
    .toIterator.map(LynxNodeLabel).flatMap { l =>
    nodes(NodeFilter(Seq(l), Map.empty))
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[LynxJDBCNode] = {
    if (nodeFilter.labels.isEmpty && nodeFilter.properties.isEmpty) return nodes()
    val t = nodeFilter.labels.head.toString
    singleTableSelect(t, nodeFilter.properties).map { rs => Mapper.mapNode(rs, t, nodeSchema(t)) }
  }

  /**
   * Get all relationships in the database
   *
   * @return Iterator[PathTriple]
   */
  override def relationships(): Iterator[PathTriple] = relSchema.keys
    .toIterator.map(LynxRelationshipType).flatMap { t =>
    relationships(RelationshipFilter(Seq(t), Map.empty))
  }

  /**
   * Get relationships with filter
   *
   * @param relationshipFilter the filter with specific conditions
   * @return Iterator[PathTriple]
   */
  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    println("relationships: ", relationshipFilter)
    val t = relationshipFilter.types.head.toString
    singleTableSelect(t, relationshipFilter.properties).map { rs =>
      PathTriple(
        nodeAt(LynxIntegerID(rs.getLong(START_ID_COL_NAME)), relMapping(t).source).get,
        Mapper.mapRel(rs, t, relSchema(t)),
        nodeAt(LynxIntegerID(rs.getLong(END_ID_COL_NAME)), relMapping(t).target).get
      )
    }
  }


  override def expand(nodeId: LynxId, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    this.expand(nodeId, relationshipFilter, direction).filter { pathTriple =>
      endNodeFilter.matches(pathTriple.endNode)
    }
  }

  /**
   * Used for path like (A)-[B]->(C), where A is point to start expand, B is relationship, C is endpoint
   *
   * @param id        id of A
   * @param filter    relationship filter of B
   * @param direction OUTGOING (-[B]->) or INCOMING (<-[B]-)
   * @return Iterator[PathTriple]
   */
  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    if (direction == BOTH) {
      return expand(id, filter, OUTGOING) ++ expand(id, filter, INCOMING)
    }

    val tableName = filter.types.head.toString
    val relType = filter.types.head.toString
    val conditions = filter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray

    val startNode = direction match {
      case OUTGOING => nodeAt(id, relMapping(tableName).source)
      case INCOMING => nodeAt(id, relMapping(tableName).target)
    }

    if (startNode.isEmpty) {
      return Iterator.empty
    }
    // start building sql query
    val selectWhat = relSchema(relType).map(_._1).mkString(",")
    val where = (direction match {
      case OUTGOING => s" where $relType.$START_ID_COL_NAME = ${id.toLynxInteger.value} "
      case INCOMING => s" where $relType.$END_ID_COL_NAME = ${id.toLynxInteger.value} "
    }) + (if (conditions.nonEmpty) " and " + conditions.mkString(" and ") else "")

    val sql = s"select $selectWhat from $relType $where"

    iterExecute(sql).map { resultSet =>
      val endNode = direction match {
        case OUTGOING => nodeAt(LynxIntegerID(resultSet.getLong(END_ID_COL_NAME)), relMapping(tableName).target).get
        case INCOMING => nodeAt(LynxIntegerID(resultSet.getLong(START_ID_COL_NAME)), relMapping(tableName).source).get
      }
      PathTriple(startNode.get, mapRel(resultSet, relType, relSchema(relType)), endNode)
    }
  }

 }


