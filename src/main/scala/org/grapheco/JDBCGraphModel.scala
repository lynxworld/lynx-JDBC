package org.grapheco

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.Mapper.mapRel
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.runner.{GraphModel, NodeFilter, RelationshipFilter, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.grapheco.schema.{NodeStructure, RelStructure, Schema, SchemaManager}
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.sql.{Connection, ResultSet}
import scala.Option.option2Iterable
import scala.collection.mutable

class JDBCGraphModel(val connection: Connection, val schema: Schema) extends GraphModel with LazyLogging {

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

  private def iterUpdate(sql: String): Unit = {
    logger.info(sql)
    val statement = connection.createStatement()
    val result = statement.executeUpdate(sql)
    //    Iterator.continually(result).takeWhile(_.n)
    logger.info(s"Rows updated: $result")
  }

  override def write: WriteTask = this._write

  val _write: WriteTask = new WriteTask {
    val _nodesBuffer: mutable.Map[LynxIntegerID, LynxJDBCNode] = mutable.Map()

    val _nodesToDelete: mutable.ArrayBuffer[LynxIntegerID] = mutable.ArrayBuffer()

    val _relationshipsBuffer: mutable.Map[LynxIntegerID, LynxJDBCRelationship] = mutable.Map()

    val _relationshipsToDelete: mutable.ArrayBuffer[LynxIntegerID] = mutable.ArrayBuffer()
    private var _nodeId: Long = 0

    private var _relationshipId: Long = 0

    private def relationshipId: LynxIntegerID = {
      _relationshipId += 1;
      LynxIntegerID(_relationshipId)
    }

    private def nodeId: LynxIntegerID = {
      _nodeId += 1
      LynxIntegerID(_nodeId)
    }

    override def createElements[T](nodesInput: Seq[(String, NodeInput)],
                                   relationshipsInput: Seq[(String, RelationshipInput)],
                                   onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = {
      val nodesMap: Map[String, LynxJDBCNode] = nodesInput.toMap
        .map { case (valueName, input) => valueName -> LynxJDBCNode(nodeId, input.labels, input.props.toMap) }

      def localNodeRef(ref: Any): LynxIntegerID = ref match {
        case StoredNodeInputRef(id) => LynxIntegerID(id.toLynxInteger.value)
        case ContextualNodeInputRef(valueName) => nodesMap(valueName).id
      }

      val relationshipsMap: Map[String, LynxJDBCRelationship] = relationshipsInput.toMap.map {
        case (valueName, input) =>
          valueName -> LynxJDBCRelationship(relationshipId, localNodeRef(input.startNodeRef),
            localNodeRef(input.endNodeRef), input.types.headOption, input.props.toMap)
      }

      _nodesBuffer ++= nodesMap.map { case (_, node) => (node.id, node) }
      _relationshipsBuffer ++= relationshipsMap.map { case (_, relationship) => (relationship.id, relationship) }
      onCreated(nodesMap.toSeq, relationshipsMap.toSeq)
    }

    override def deleteRelations(ids: Iterator[LynxId]): Unit = {
      if (ids.nonEmpty) {
        val tableNames: Seq[String] = schema.relSchema.map(_.table_name)
        val idList = ids.mkString(",")
        for (tableName <- tableNames) {
          val sql = s"DELETE FROM $tableName WHERE id IN ($idList)"
          iterUpdate(sql)
        }
      }
    }

    override def deleteNodes(ids: Seq[LynxId]): Unit = {
      deleteRelations(ids.iterator)
      if (ids.nonEmpty) {
        val tableNames: Seq[String] = schema.nodeSchema.map(_.table_name)
        val idList = ids.mkString(",")
        for (tableName <- tableNames) {
          val sql = s"DELETE FROM $tableName WHERE id IN ($idList)"
          iterUpdate(sql)
        }
      }
    }

    def updateTable(lynxId: LynxId, table: Any, propMap: Map[LynxPropertyKey, LynxValue]): Iterator[ResultSet] = {
      val (tableName, props, idCol) = table match {
        case v: NodeStructure => (v.table_name, v.properties, v.id)
        case v: RelStructure => (v.table_name, v.properties, v.table_id)
      }
      val updateSql = s"UPDATE $tableName SET "
      //      var i
      val valueSql =
        propMap.map { case (key, value) =>
          if (props.foldLeft(Map.empty[String, String]) {
            case (acc, (key, value)) => acc + (key -> value)
          }(key.value).nonEmpty)
            s"${key.value} = '${value.toString}'"
        }.mkString(", ") + s" WHERE  $idCol = $lynxId"
      iterUpdate(updateSql + valueSql)
      iterExecute(s"select * from $tableName WHERE $idCol = $lynxId")
    }

    override def updateNode(lynxId: LynxId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]): Option[LynxNode] = {
      SchemaManager.findNodeByLabel(schema, labels.head.value) match {
        case Some(table) => updateTable(lynxId, table, props)
        case _ => None
      }
      nodeAt(lynxId)
    }

    override def updateRelationShip(lynxId: LynxId, props: Map[LynxPropertyKey, LynxValue]): Option[LynxRelationship] = {
      SchemaManager.findRelByLabel(schema, nodeAt(lynxId).get.labels.head.value) match {
        case Some(table) => updateTable(lynxId, table, props)
          Option(mapRel(updateTable(lynxId, table, props).toSeq.head, table.table_name, schema))
        case _ => None
      }
    }

    override def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)], cleanExistProperties: Boolean): Iterator[Option[LynxNode]] = {
      if (cleanExistProperties) {
        deleteNodes(nodeIds.toSeq)
      }
      val props: Map[LynxPropertyKey, LynxValue] = data.foldLeft(Map.empty[LynxPropertyKey, LynxValue]) {
        case (acc, (key, value)) => acc + (key -> LynxValue(value))
      }
      nodeIds.map { nodeId =>
        updateNode(nodeId, nodeAt(nodeId).get.labels, props)
      }
    }

    override def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = {
      nodeIds.map { nodeId =>
        updateNode(nodeId, labels, null)
      }
    }

    override def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)]): Iterator[Option[LynxRelationship]] = {
      val props: Map[LynxPropertyKey, LynxValue] = data.foldLeft(Map.empty[LynxPropertyKey, LynxValue]) {
        case (acc, (key, value)) => acc + (key -> LynxValue(value))
      }
      relationshipIds.map { nodeId =>
        updateRelationShip(nodeId, props)
      }
    }

    override def setRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = {
      relationshipIds.map { nodeId =>
        val n = SchemaManager.findRelByLabel(schema, nodeAt(nodeId).get.labels.head.value).get
        updateRelationShip(nodeId, Map(LynxPropertyKey(n.label) -> LynxValue(typeName.value)))
      }
    }

    override def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxNode]] = {
      nodeIds.map { nodeId =>
        val n = SchemaManager.findNodeByLabel(schema, nodeAt(nodeId).get.labels.head.value).get
        updateNode(nodeId, Seq(LynxNodeLabel(n.label)), data.map { label =>
          val propertyKey = LynxPropertyKey(label.value)
          propertyKey -> null
        }.toMap)
      }
    }

    override def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = {
      nodeIds.map { nodeId =>
        val n = SchemaManager.findNodeByLabel(schema, nodeAt(nodeId).get.labels.head.value).get
        updateNode(nodeId, Seq(LynxNodeLabel(n.label)), labels.map { label =>
          val propertyKey = LynxPropertyKey(label.value)
          propertyKey -> null
        }.toMap)
      }
    }

    override def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxRelationship]] = {
      relationshipIds.map { nodeId =>
        val n = SchemaManager.findRelByLabel(schema, nodeAt(nodeId).get.labels.head.value).get
        updateRelationShip(nodeId, data.map { label =>
          val propertyKey = LynxPropertyKey(label.value)
          propertyKey -> null
        }.toMap)
      }
    }

    override def removeRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = {
      val tableName = SchemaManager.findRelByLabel(schema, typeName.value).get.table_name
      if (relationshipIds.nonEmpty) {
        val idList = relationshipIds.mkString(",")
        val sql = s"DELETE FROM $tableName WHERE id IN ($idList)"
        iterUpdate(sql)
        val newRel = iterExecute(s"select * from $tableName WHERE id = $idList  ")
        Iterator(Option(mapRel(newRel.toSeq.head, tableName, schema).value))
      } else {
        throw new Exception("relationshipIds is empty")
      }
    }

    override def commit: Boolean = true
  }

  override def nodeAt(id: LynxId): Option[LynxNode] = {
    val nameList: Seq[String] = schema.nodeSchema.map(_.table_name)
    nodeAt(id, nameList)
  }

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
      val nodeId = SchemaManager.findNodeByName(schema, t).map(_.id).get
      singleTableSelect(t, List((nodeId, id.toLynxInteger.v)))
        .map(rs => Mapper.mapNode(rs, t, schema)).toSeq.headOption
    }.headOption
  }

  override def nodes(): Iterator[LynxJDBCNode] = schema.nodeSchema.toIterator.flatMap { nodeSchema =>
    val labels = if (nodeSchema.fix_label) Seq(LynxNodeLabel(nodeSchema.label)) else Seq.empty
    nodes(NodeFilter(labels, Map.empty))
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[LynxJDBCNode] = {
    if (nodeFilter.labels.isEmpty && nodeFilter.properties.isEmpty) return nodes()
    val label = nodeFilter.labels.head.toString
    val filteredSchema = SchemaManager.findNodeByName(schema, label)
    filteredSchema match {
      case Some(schemas) =>
        singleTableSelect(label, nodeFilter.properties).map { rs =>
          Mapper.mapNode(rs, schemas.table_name, schema)
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

  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    println("relationships: ", relationshipFilter)
    val t = relationshipFilter.types.head.toString
    singleTableSelect(t, relationshipFilter.properties).map { rs =>
      val (srcColName, dstColName) =
        SchemaManager.findRelByName(schema, t).map(node => (node.f1_name, node.f2_name)).get
      PathTriple(
        nodeAt(LynxIntegerID(rs.getLong(srcColName)), schema.relMapping(t).source).get,
        Mapper.mapRel(rs, t, schema),
        nodeAt(LynxIntegerID(rs.getLong(dstColName)), schema.relMapping(t).target).get
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
    val conditions = filter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray

    val startNode = direction match {
      case OUTGOING => nodeAt(id, schema.relMapping(tableName).source)
      case INCOMING => nodeAt(id, schema.relMapping(tableName).target)
    }
    if (startNode.isEmpty) {
      return Iterator.empty
    }
    val selectWhat = schema.relSchema.find(_.table_name == tableName).map(_.properties).get.map(_._1).mkString(",")
    val (srcColName, dstColName) =
      SchemaManager.findRelByName(schema, tableName).map(node => (node.f1_name, node.f2_name)).get
    val where = (direction match {
      case OUTGOING => s" where $tableName.$srcColName = ${id.toLynxInteger.value} "
      case INCOMING => s" where $tableName.$dstColName = ${id.toLynxInteger.value} "
    }) + (if (conditions.nonEmpty) " and " + conditions.mkString(" and ") else "")
    val sql = s"select $selectWhat from $tableName $where"
    iterExecute(sql).map { resultSet =>
      val endNode = direction match {
        case OUTGOING => nodeAt(LynxIntegerID(resultSet.getLong(dstColName)), schema.relMapping(tableName).target).get
        case INCOMING => nodeAt(LynxIntegerID(resultSet.getLong(srcColName)), schema.relMapping(tableName).source).get
      }
      PathTriple(startNode.get, mapRel(resultSet, tableName, schema), endNode)
    }
  }
}


