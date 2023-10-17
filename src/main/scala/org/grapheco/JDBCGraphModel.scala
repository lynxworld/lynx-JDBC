package org.grapheco

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.Mapper.mapRel
import org.grapheco.db.DB
import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.runner.{CypherRunner, GraphModel, NodeFilter, RelationshipFilter, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType, PathTriple}
//import org.grapheco.schema.ToSchema._
import org.grapheco.schema.SchemaManager
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.sql.{Connection, ResultSet}

class JDBCGraphModel(val connection: Connection) extends GraphModel with LazyLogging {
  //  ToSchema().init(connection)

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

  val schema = SchemaManager.autoGeneration(connection)

  override def nodeAt(id: LynxId): Option[LynxNode] = ???

  private def singleTableSelect(tableName: String, filters: Map[LynxPropertyKey, LynxValue]): Iterator[ResultSet] = {
    val conditions: Map[String, Any] = filters.map { case (k, v) => k.toString -> v.value }
    singleTableSelect(tableName, conditions.toList)
  }

  private def singleTableSelect(tableName: String, conditions: Seq[(String, Any)]): Iterator[ResultSet] = {
    val node_schema = schema.nodeSchema.find(_.table_name == tableName)

    node_schema match {
      case Some(tableStructure) =>
        val selectWhat = tableStructure.properties.map(_._1).mkString(",")
        val sql = s"select $selectWhat from $tableName ${
          if (conditions.isEmpty) ""
          else " where " + conditions.map { case (key, value) => s"$key = '$value'" }.mkString(" and ")
        }"
        iterExecute(sql)

      case None =>
        Iterator.empty
    }
  }

  private def nodeAt(id: LynxId, tableList: Seq[String]): Option[LynxJDBCNode] = {
    tableList.flatMap { t =>
      //      singleTableSelect(t, List((ID_COL_NAME, id.toLynxInteger.v)))
      singleTableSelect(t, List((schema.nodeSchema.find(_.table_name == t).map(_.id).get, id.toLynxInteger.v)))
        .map(rs => Mapper.mapNode(rs, t, schema.nodeSchema.find(_.table_name == t).map(_.properties).get, schema.nodeSchema.find(_.table_name == t).map(_.id).get)).toSeq.headOption
    }.headOption
  }

  override def nodes(): Iterator[LynxJDBCNode] = schema.nodeSchema.toIterator.flatMap { nodeSchema =>
    val labels = if (nodeSchema.fix_label) Seq(LynxNodeLabel(nodeSchema.label)) else Seq.empty
    nodes(NodeFilter(labels, Map.empty))
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[LynxJDBCNode] = {
    if (nodeFilter.labels.isEmpty && nodeFilter.properties.isEmpty) return nodes()
    val label = nodeFilter.labels.head.toString
    val filteredSchema = schema.nodeSchema.find(_.table_name == label)

    filteredSchema match {
      case Some(schemas) =>
        singleTableSelect(label, nodeFilter.properties).map { rs =>
          Mapper.mapNode(rs, schemas.table_name, schemas.properties, schema.nodeSchema.find(_.table_name == schemas.table_name).map(_.id).get)
        }
      case None =>
        Iterator.empty
    }
  }


  /**
   * Get all relationships in the database
   *
   * @return Iterator[PathTriple]
   */
  override def relationships(): Iterator[PathTriple] = {
    schema.relSchema.flatMap { schema =>
      val t = LynxRelationshipType(schema.table_name)
      relationships(RelationshipFilter(Seq(t), Map.empty))
    }.toIterator
  }

//  /**
//   * Get relationships with filter
//   *
//   * @param relationshipFilter the filter with specific conditions
//   * @return Iterator[PathTriple]
//   */
//  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
//    println("relationships: ", relationshipFilter)
//    val t = relationshipFilter.types.head.toString
//    singleTableSelect(t, relationshipFilter.properties).map { rs =>
//      PathTriple(
//        //        nodeAt(LynxIntegerID(rs.getLong(START_ID_COL_NAME)), schema.relMapping(t).source).get,
//        nodeAt(LynxIntegerID(rs.getLong(schema.relSchema.find(_.table_name == t).map(_.f1_name).get)), schema.relMapping(t).source).get,
//        Mapper.mapRel(rs, t, schema.relSchema.find(_.table_name == t) match {
//          case Some(table) => (table.table_id, table.f1_name, table.f2_name)
//        }
//          , schema.relSchema.find(_.table_name == t).map(_.properties).get)
//        ,
//        //        nodeAt(LynxIntegerID(rs.getLong(END_ID_COL_NAME)), schema.relMapping(t).target).get
//        nodeAt(LynxIntegerID(rs.getLong(schema.relSchema.find(_.table_name == t).map(_.f2_name).get)), schema.relMapping(t).target).get
//      )
//    }
//  }

  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    println("relationships: ", relationshipFilter)
    val t = relationshipFilter.types.head.toString
    singleTableSelect(t, relationshipFilter.properties).map { rs =>
      PathTriple(
        nodeAt(LynxIntegerID(rs.getLong(schema.relSchema.find(_.table_name == t).map(_.f1_name).get)), schema.relMapping(t).source).get,
        Mapper.mapRel(rs, t, (schema.relSchema.find(_.table_name == t).map(_.table_id).get, schema.relSchema.find(_.table_name == t).map(_.f1_name).get, schema.relSchema.find(_.table_name == t).map(_.f2_name).get),schema.relSchema.find(_.table_name == t).map(_.properties).get),
        nodeAt(LynxIntegerID(rs.getLong(schema.relSchema.find(_.table_name == t).map(_.f2_name).get)), schema.relMapping(t).target).get
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
      case OUTGOING => nodeAt(id, schema.relMapping(tableName).source)
      case INCOMING => nodeAt(id, schema.relMapping(tableName).target)
    }

    if (startNode.isEmpty) {
      return Iterator.empty
    }
    val selectWhat = schema.relSchema.find(_.table_name == relType).map(_.properties).get.map(_._1).mkString(",")
    val relTable = schema.relSchema.find(_.table_name == tableName)
    val srcColName = relTable.map(_.f1_name).get
    val dstColName = relTable.map(_.f2_name).get
    val where = (direction match {
      case OUTGOING => s" where $relType.$srcColName = ${id.toLynxInteger.value} "
      case INCOMING => s" where $relType.$dstColName = ${id.toLynxInteger.value} "
    }) + (if (conditions.nonEmpty) " and " + conditions.mkString(" and ") else "")

    val sql = s"select $selectWhat from $relType $where"

    iterExecute(sql).map { resultSet =>
      val endNode = direction match {
        case OUTGOING => nodeAt(LynxIntegerID(resultSet.getLong(dstColName)), schema.relMapping(tableName).target).get
        case INCOMING => nodeAt(LynxIntegerID(resultSet.getLong(srcColName)), schema.relMapping(tableName).source).get
      }
      PathTriple(startNode.get, mapRel(resultSet, relType, (relTable.map(_.table_id).get, relTable.map(_.f1_name).get, relTable.map(_.f2_name).get), schema.relSchema.find(_.table_name == relType).map(_.properties).get), endNode)
    }
  }

}


