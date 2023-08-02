//Scala-sdk-2.12.17
package org.example

import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.runner.{CypherRunner, GraphModel, NodeFilter, RelationshipFilter, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationship, LynxRelationshipType, PathTriple}
import org.grapheco.lynx.types.time.LynxDate
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.time.LocalDate
import java.util.Date
import java.sql.{Connection, DriverManager, ResultSet}

class MyGraph extends GraphModel {

  val url = "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "root"
  val password = "Hc1478963!"
  Class.forName(driver)

  val connection: Connection = DriverManager.getConnection(url, username, password)

  /**
   * Transform global date to local date
   *
   * @param date Global date
   * @return LocalDate
   */
  private def transDate(date: Date): LocalDate = {
    LocalDate.ofEpochDay(date.getTime / 86400000)
  }

  /**
   * Transform a row in MySQL node table to LynxNode
   *
   * @param row       A ResultSet type object in MySQL
   * @param tableName Name of MySQL node table
   * @param config    MySQL node table schema
   * @return MyNode
   */
  private def rowToNode(row: ResultSet, tableName: String, config: Array[(String, String)]): MyNode = {
    // fill properties according to the schema
    val propertyMap = config.indices.map { i =>
      val columnName = LynxPropertyKey(config(i)._1)
      val columnType = config(i)._2
      val columnValue =
        try {
          columnType match {
            case "String" => LynxString(row.getString(i + 1))
            case "BIGINT" => LynxValue(row.getLong(i + 1))
            case "INT" => LynxValue(row.getInt(i + 1))
            case "Date" => LynxDate(transDate(row.getDate(i + 1)))
            case _ => LynxString(row.getString(i + 1))
          }
        } catch {
          // Use LynxNull to replace None value
          case _: NullPointerException => LynxNull
        }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong("id:ID"))
    val label = Seq(LynxNodeLabel(tableName))
    MyNode(id, label, propertyMap)
  }

  /**
   * Transform part of a row in MySQL node table to LynxNode
   *
   * @param row       A ResultSet type object in MySQL
   * @param tableName Name of MySQL node table
   * @param config    MySQL node table schema
   * @param offset    Number of columns to skip
   * @return MyNode
   */
  private def rowToNodeOffset(row: ResultSet, tableName: String, config: Array[(String, String)], offset: Int): MyNode = {
    // fill properties according to the schema
    val propertyMap = config.indices.map { i =>
      val columnName = LynxPropertyKey(config(i)._1)
      val columnType = config(i)._2
      val index = i + offset + 1
      val columnValue =
        try {
          columnType match {
            case "String" => LynxString(row.getString(index))
            case "BIGINT" => LynxValue(row.getLong(index))
            case "INT" => LynxValue(row.getInt(index))
            case "Date" => LynxDate(transDate(row.getDate(index)))
            case _ => LynxString(row.getString(index))
          }
        } catch {
          // Use LynxNull to replace None value
          case _: NullPointerException => LynxNull
        }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong(offset + 1))
    val label = Seq(LynxNodeLabel(tableName))
    MyNode(id, label, propertyMap)
  }

  /**
   * Transform a row in MySQL relationship table to LynxRelationship
   *
   * @param row     A ResultSet type object in MySQL
   * @param relName Name of MySQL relationship table
   * @param config  MySQL relationship table schema
   * @return MyRelationship
   */
  private def rowToRel(row: ResultSet, relName: String, config: Array[(String, String)]): MyRelationship = {
    // fill properties according to the schema
    val propertyMap = config.indices.map { i =>
      val columnName = LynxPropertyKey(config(i)._1)
      val columnType = config(i)._2
      val columnValue =
        try {
          columnType match {
            case "String" => LynxString(row.getString(i + 1))
            case "BIGINT" => LynxValue(row.getLong(i + 1))
            case "INT" => LynxValue(row.getInt(i + 1))
            case "Date" => LynxDate(transDate(row.getDate(i + 1)))
            case _ => LynxString(row.getString(i + 1))
          }
        } catch {
          // Use LynxNull to replace None value
          case _: NullPointerException => LynxNull
        }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong("REL_ID"))
    val startId = MyId(row.getLong(":START_ID"))
    val endId = MyId(row.getLong(":END_ID"))

    MyRelationship(id, startId, endId, Some(LynxRelationshipType(relName)), propertyMap)
  }

  /**
   *
   * @param row     A ResultSet type object in MySQL
   * @param relName Name of MySQL relationship table
   * @param config  MySQL relationship table schema
   * @param offset  Number of columns to skip
   * @return MyRelationship
   */
  private def rowToRelOffset(row: ResultSet, relName: String, config: Array[(String, String)], offset: Int): MyRelationship = {
    // fill properties according to the schema
    val propertyMap = config.indices.map { i =>
      val columnName = LynxPropertyKey(config(i)._1)
      val columnType = config(i)._2
      val index = i + offset + 1
      val columnValue =
        try {
          columnType match {
            case "String" => LynxString(row.getString(index))
            case "BIGINT" => LynxValue(row.getLong(index))
            case "INT" => LynxValue(row.getInt(index))
            case "Date" => LynxDate(transDate(row.getDate(index)))
            case _ => LynxString(row.getString(index))
          }
        } catch {
          // Use LynxNull to replace None value
          case _: NullPointerException => LynxNull
        }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong("REL_ID"))
    val startId = MyId(row.getLong("START_ID"))
    val endId = MyId(row.getLong("END_ID"))

    MyRelationship(id, startId, endId, Some(LynxRelationshipType(relName)), propertyMap)
  }

  //Assume each table has a column "id:ID" as the primary key
  private val nodeSchema = Map(
    "Person" -> Array(("id", "BIGINT"), (":LABEL", "String"), ("creationDate", "Date"), ("firstName", "String"),
      ("lastName", "String"), ("gender", "String"), ("birthday", "Date"), ("locationIP", "String"), ("browserUsed", "String"),
      ("languages", "String"), ("emails", "String")),
    "Place" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String"), ("type", "String")),
    "Organisation" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("type", "String"), ("name", "String"), ("url", "String")),
    "Comment" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("creationDate", "Date"), ("locationIP", "String"),
      ("browserUsed", "String"), ("content", "String"), ("length", "INT")),
    "Post" -> Array(("id:ID", "BIGINT"), ("creationDate", "Date"), (":LABEL", "String"), ("imageFile", "String"),
      ("locationIP", "String"), ("browserUsed", "String"), ("language", "String"), ("content", "String"), ("length", "INT")),
    "Forum" -> Array(("id:ID", "BIGINT"), ("creationDate", "Date"), (":LABEL", "String"), ("title", "String")),
    "Tag" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String")),
    "Tagclass" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String"))
  )

  //Assume each table has a column "REL_ID" as the primary key, a column ":START_ID" as the start of rel,
  // a column ":END_ID" as the end of rel
  private val relSchema = Map(
    "knows" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isLocatedIn" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("creationDate", "Date")),
    "containerOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasCreator" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasInterest" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasMember" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasModerator" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasTag" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasType" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isPartOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isSubclassOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "likes" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "replyOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "studyAt" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("classYear", "INT")),
    "workAt" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("workFrom", "INT"))
  )

  private val relMapping = Map(
    "isLocatedIn" -> (Array("Person", "Comment", "Post", "Organisation"), Array("Place")),
    "replyOf" -> (Array("Comment"), Array("Comment", "Post")), "containerOf" -> (Array("Forum"), Array("Post")),
    "hasCreator" -> (Array("Comment", "Post"), Array("Person")), "hasInterest" -> (Array("Person"), Array("Tag")),
    "workAt" -> (Array("Person"), Array("Organisation")), "hasModerator" -> (Array("Forum"), Array("Person")),
    "hasTag" -> (Array("Comment", "Post", "Forum"), Array("Tag")), "hasType" -> (Array("Tag"), Array("Tagclass")),
    "isSubclassOf" -> (Array("Tagclass"), Array("Tagclass")), "isPartOf" -> (Array("Place"), Array("Place")),
    "likes" -> (Array("Person"), Array("Comment", "Post")), "knows" -> (Array("Person"), Array("Person")),
    "studyAt" -> (Array("Person"), Array("Organisation")), "hasMember" -> (Array("Forum"), Array("Person")),
  )

  //Inherit methods from GraphModel class
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

    override def commit: Boolean = {
      true
    }
  }

  override def nodeAt(id: LynxId): Option[LynxNode] = ???

  /**
   * Find the Node given a specific id
   *
   * @param id        Node id
   * @param tableList Tables the node might exist
   * @return MyNode
   */
  private def myNodeAt(id: LynxId, tableList: Array[String]): Option[MyNode] = {
    println("myNodeAt()")

    for (tableName <- tableList) {
      val statement = connection.createStatement
      val sql = s"select * from $tableName where `id:ID` = ${id.toLynxInteger.value}"

      println(sql)

      val startTime2 = System.currentTimeMillis()
      val row = statement.executeQuery(sql)
      println("myNodeAt() SQL used: " + (System.currentTimeMillis() - startTime2) + " ms")

      // the node is found
      if (row.next()) {
        val result = rowToNode(row, tableName, nodeSchema(tableName))
        return Some(result)
      }
    }
    None
  }

  /**
   * Get all nodes in the database
   *
   * @return Iterator[Node]
   */
  override def nodes(): Iterator[MyNode] = {
    println("nodes()")

    // iterate all node tables
    val allNodes = for (tableName <- nodeSchema.keys) yield {
      val statement = connection.createStatement
      val data = statement.executeQuery(s"select * from $tableName")

      // transform all rows in the table to LynxNode
      Iterator.continually(data).takeWhile(_.next())
        .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }
    }

    println("nodes() finished")
    allNodes.flatten.iterator
  }

  /**
   * Get nodes with filter
   *
   * @param nodeFilter the filter with specific conditions
   * @return Iterator[Node]
   */
  override def nodes(nodeFilter: NodeFilter): Iterator[MyNode] = {
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

    val statement = connection.createStatement
    val startTime2 = System.currentTimeMillis()
    val data = statement.executeQuery(sql)
    println("nodes(nodeFilter) SQL used: " + (System.currentTimeMillis() - startTime2) + " ms")

    // transform the rows in the sql result to LynxNodes
    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }

    result
  }

  /**
   * Get all relationships in the database
   *
   * @return Iterator[PathTriple]
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

  /**
   * Get relationships with filter
   *
   * @param relationshipFilter the filter with specific conditions
   * @return Iterator[PathTriple]
   */
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
        val startNode = myNodeAt(MyId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
        val endNode = myNodeAt(MyId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
        val rel = rowToRel(resultSet, tableName, relSchema(tableName))

        PathTriple(startNode, rel, endNode)
      }

    result
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
    println("expand()")

    if (direction == BOTH) {
      return expand(id, filter, OUTGOING) ++ expand(id, filter, INCOMING)
    }

    val tableName = filter.types.head.toString
    val relType = filter.types.head.toString
    val conditions = filter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray

    val startNode = direction match {
      case OUTGOING => myNodeAt(id, relMapping(tableName)._1)
      case INCOMING => myNodeAt(id, relMapping(tableName)._2)
    }

    if (startNode.isEmpty) {
      return Iterator.empty
    }

    // start building sql query
    var sql = s"select * from $relType"
    direction match {
      case OUTGOING => sql = sql + s" where $relType.`:START_ID` = ${id.toLynxInteger.value} "
      case INCOMING => sql = sql + s" where $relType.`:END_ID` = ${id.toLynxInteger.value} "
    }

    for (i <- conditions.indices) {
      sql = sql + " and " + conditions(i)
    }

    println(sql)

    // get sql query result
    val statement = connection.createStatement
    val startTime1 = System.currentTimeMillis()
    val data = statement.executeQuery(sql)
    println("expand() SQL used: " + (System.currentTimeMillis() - startTime1) + " ms")

    // transform rows in the result to PathTriples
    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet =>
        val endNode = direction match {
          case OUTGOING => myNodeAt(MyId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
          case INCOMING => myNodeAt(MyId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
        }

        PathTriple(startNode.get, rowToRel(resultSet, relType, relSchema(relType)), endNode)
      }

    println("expand() totally used: " + (System.currentTimeMillis() - startTime1) + " ms")
    result
  }

  /**
   * Used for path like (A)-[B]->(C), where A is point to start expand, B is relationship, C is endpoint
   *
   * @param nodeId        id of A
   * @param filter        relationship filter of B
   * @param endNodeFilter node filter of C
   * @param direction     OUTGOING (-[B]->) or INCOMING (<-[B]-)
   * @return Iterator[PathTriple]
   */
  override def expand(nodeId: LynxId, filter: RelationshipFilter,
                      endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    val result = expand(nodeId, filter, direction).filter {
      pathTriple => endNodeFilter.matches(pathTriple.endNode)
    }

    result
  }

  /**
   * Used for path like (A)-[B]->(C), but here A may be more than one node
   * Allow multi-hop queries like (A)-[B*0..]->(C)
   *
   * @param startNodeFilter    node filter of A
   * @param relationshipFilter relationship filter of B
   * @param endNodeFilter      node filter of C
   * @param direction          OUTGOING (-[B]->) or INCOMING (<-[B]-)
   * @param upperLimit         maximum length of path
   * @param lowerLimit         minimum length of path
   * @return Iterator[LynxPath]
   */
  override def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter,
                     direction: SemanticDirection, upperLimit: Int, lowerLimit: Int): Iterator[LynxPath] = {
    //TODO: 暂不支持多跳的情况
    if (upperLimit != 1 || lowerLimit != 1) {
      throw new RuntimeException("Upper limit or lower limit not support")
    }

    val startTime1 = System.currentTimeMillis()

    if (direction == BOTH) {
      return paths(startNodeFilter, relationshipFilter, endNodeFilter, OUTGOING, upperLimit, lowerLimit) ++
        paths(startNodeFilter, relationshipFilter, endNodeFilter, INCOMING, upperLimit, lowerLimit)
    }

    val relType = relationshipFilter.types.head.toString
    val startTables = if (startNodeFilter.labels.nonEmpty) {
      startNodeFilter.labels.map(_.toString).toArray
    }
    else {
      relMapping(relType)._1
    }
    val endTables = if (endNodeFilter.labels.nonEmpty) {
      endNodeFilter.labels.map(_.toString).toArray
    }
    else {
      relMapping(relType)._2
    }


    // iterate through all possible combinations
    val finalResult = for {
      startTable: String <- startTables
      endTable: String <- endTables
    } yield {
      var sql1 = s"select * from (select * from $startTable"
      var sql2 = s" )as t1 join $relType on t1.id = "
      val dirsql = direction match {
        case OUTGOING => s"$relType.START_ID join $endTable as t2 on t2.id = $relType.END_ID"
        case INCOMING => s"$relType.END_ID join $startTable as t2 on t2.id = $relType.START_ID"
      }
      val conditions = Array.concat(
        startNodeFilter.properties.map { case (key, value) => s" $startTable.`${key.toString}` = '${value.value}'" }.toArray,
        relationshipFilter.properties.map { case (key, value) => s"$relType.`${key.toString}` = '${value.value}'" }.toArray,
        endNodeFilter.properties.map { case (key, value) => s"t2.`${key.toString}` = '${value.value}'" }.toArray
      )
      for (i <- conditions.indices) {
        if (i == 0) {
          sql1 = sql1 + " where " + conditions(i) + sql2 + dirsql+";"
        } else {
          sql1 = sql1 + " and " + conditions(i)
        }
      }

      println(sql1)

      val statement = connection.createStatement
      val startTime1 = System.currentTimeMillis()
      val data = statement.executeQuery(sql1)
      println("paths() combined SQL used: " + (System.currentTimeMillis() - startTime1) + " ms")

      // transform rows in the result to LynxPaths
      Iterator.continually(data).takeWhile(_.next()).map { resultSet =>
        PathTriple(
          rowToNodeOffset(resultSet, startTable, nodeSchema(startTable), 0),
          rowToRelOffset(resultSet, relType, relSchema(relType), nodeSchema(startTable).length),
          rowToNodeOffset(resultSet, endTable, nodeSchema(endTable), nodeSchema(startTable).length + relSchema(relType).length)
        ).toLynxPath
      }
    }

    println("paths() combined totally used: " + (System.currentTimeMillis() - startTime1) + " ms")
    finalResult.iterator.flatten
  }

  // 原来的paths()
  /*  override def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter,
              direction: SemanticDirection, upperLimit: Int, lowerLimit: Int): Iterator[LynxPath] = {
      // println("paths()" + upperLimit + " " + lowerLimit)
      // 先不考虑多跳的情况
  //    if (upperLimit != 1 || lowerLimit != 1) {
  //      throw new RuntimeException("Upper limit or lower limit not support")
  //    }

  //    if (startNodeFilter.properties.size == 0) {
  //      val result = relationships(relationshipFilter).map(_.toLynxPath)
  //                      .filter(_.endNode.forall(endNodeFilter.matches))
  //      println("paths() finished 2")
  //      return result
  //    }

      val originStations = nodes(startNodeFilter)
      val result = originStations.flatMap { originStation =>
         val firstStop = expandNonStop(originStation, relationshipFilter, direction, lowerLimit)
         val leftSteps = Math.min(upperLimit, 100) - lowerLimit
         firstStop.flatMap(p => extendPath(p, relationshipFilter, direction, leftSteps))
        // expandNonStop(originStation, relationshipFilter, direction, lowerLimit)
      }.filter(_.endNode.forall(endNodeFilter.matches))

      // println("paths() finished 1")
      result
    }*/

  private val runner = new CypherRunner(this)

  def run(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = runner.run(query, param)

}


