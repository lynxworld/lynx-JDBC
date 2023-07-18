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

  private def transDate(date: Date): LocalDate = {
    LocalDate.ofEpochDay(date.getTime/86400000)
  }

  //TODO: NA value
  private def rowToNode(row:  ResultSet, tableName: String, config: Array[(String, String)]): MyNode = {
    val propertyMap = config.indices.map { i =>
      val columnName = LynxPropertyKey(config(i)._1)
      val columnType = config(i)._2
      val columnValue = columnType match {
        case "BIGINT" => LynxValue(row.getLong(i+1))
        case "INT" => LynxInteger(row.getInt(i+1))
        case "Date" => LynxDate(transDate(row.getDate(i+1)))
        case "String" => LynxString(row.getString(i+1))
        case _ => LynxString(row.getString(i+1))
      }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong("id:ID"))
    val label = Seq(LynxNodeLabel(tableName))
    MyNode(id, label, propertyMap)
  }

  private def rowToNodeOffset(row:  ResultSet, tableName: String, config: Array[(String, String)], offset: Int): MyNode = {
    val propertyMap = config.indices.map { i =>
      val columnName = LynxPropertyKey(config(i)._1)
      val columnType = config(i)._2
      val index = i + offset + 1
      val columnValue = columnType match {
        case "BIGINT" => LynxValue(row.getLong(index))
        case "INT" => LynxInteger(row.getInt(index))
        case "Date" => LynxDate(transDate(row.getDate(index)))
        case "String" => LynxString(row.getString(index))
        case _ => LynxString(row.getString(index))
      }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong(offset + 1))
    val label = Seq(LynxNodeLabel(tableName))
    MyNode(id, label, propertyMap)
  }

  private def rowToRel(row: ResultSet, relName: String, config: Array[(String, String)]): MyRelationship = {
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
          case _: NullPointerException => LynxNull
        }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong("REL_ID"))
    val startId = MyId(row.getLong(":START_ID"))
    val endId = MyId(row.getLong(":END_ID"))

    MyRelationship(id, startId, endId, Some(LynxRelationshipType(relName)), propertyMap)
  }

  private def rowToRelOffset(row: ResultSet, relName: String, config: Array[(String, String)], offset: Int): MyRelationship = {
    val propertyMap = config.indices.map { i =>
      val columnName = LynxPropertyKey(config(i)._1)
      val columnType = config(i)._2
      val columnValue =
        try {
          columnType match {
            case "String" => LynxString(row.getString(i + offset + 1))
            case "BIGINT" => LynxValue(row.getLong(i + offset + 1))
            case "INT" => LynxValue(row.getInt(i + offset + 1))
            case "Date" => LynxDate(transDate(row.getDate(i + offset + 1)))
            case _ => LynxString(row.getString(i + offset + 1))
          }
        } catch {
          case _: NullPointerException => LynxNull
        }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong("REL_ID"))
    val startId = MyId(row.getLong(":START_ID"))
    val endId = MyId(row.getLong(":END_ID"))

    MyRelationship(id, startId, endId, Some(LynxRelationshipType(relName)), propertyMap)
  }

  //Assume each table has a column "id:ID" as the primary key
  private val nodeSchema = Map(
    "Person" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("creationDate", "Date"), ("firstName", "String"),
      ("lastName", "String"), ("gender", "String"), ("birthday", "Date"), ("locationIP", "String"), ("browserUsed", "String"),
      ("languages", "String"), ("emails", "String")),
    "Place" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String"), ("type", "String")),
    "Organisation"  -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("type", "String"), ("name", "String"), ("url", "String")),
    "Comment" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("creationDate", "Date"), ("locationIP", "String"),
      ("browserUsed", "String"), ("content", "String"), ("length", "INT")),
    "Post" -> Array(("id:ID", "BIGINT"), ("creationDate", "Date"), (":LABEL", "String"), ("imageFile", "String"),
      ("locationIP", "String"), ("browserUsed", "String"), ("language", "String"), ("content", "String"), ("length", "INT")),
    "Forum" -> Array(("id:ID", "BIGINT"), ("creationDate", "Date"), (":LABEL", "String"), ("title", "String")),
    "Tag" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String")),
    "Tagclass" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String"))
  )

//  val nodeSchema2 = Map(
//    "Person" -> Map("id" -> 1)
//  )

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
    "replyOf" -> (Array("Comment"), Array("Comment", "Post")),   "containerOf" -> (Array("Forum"), Array("Post")),
    "hasCreator" -> (Array("Comment", "Post"), Array("Person")), "hasInterest" -> (Array("Person"), Array("Tag")),
    "workAt" -> (Array("Person"), Array("Organisation")),        "hasModerator" -> (Array("Forum"), Array("Person")),
    "hasTag" -> (Array("Comment", "Post", "Forum"), Array("Tag")),   "hasType" -> (Array("Tag"), Array("Tagclass")),
    "isSubclassOf" -> (Array("Tagclass"), Array("Tagclass")),    "isPartOf" -> (Array("Place"), Array("Place")),
    "likes" -> (Array("Person"), Array("Comment", "Post")),      "knows" -> (Array("Person"), Array("Person")),
    "studyAt" -> (Array("Person"), Array("Organisation")),       "hasMember" -> (Array("Forum"), Array("Person")),
  )

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

    override def commit: Boolean = {true}
  }

  override def nodeAt(id: LynxId): Option[MyNode] = ??? /*{
    println("nodeAt()")
    for (tableName <- nodeSchema.keys) {
      val statement = connection.createStatement
      val row = statement.executeQuery(s"select * from ${tableName} where `id:ID` = ${id.toLynxInteger.value}")
      if (row.next()) {
        val result = rowToNode(row, tableName, nodeSchema(tableName))
        println("nodeAt() finished")
        return Some(result)
      }
    }
    println("nodeAt() finished")
    return None
  }*/

  private def myNodeAt(id: LynxId, tableList:Array[String]): Option[MyNode] = {
    println("myNodeAt()")
    val startTime1 = System.currentTimeMillis()

    for (tableName <- tableList) {
      val statement = connection.createStatement
      val sql = s"select * from $tableName where `id:ID` = ${id.toLynxInteger.value}"

      println(sql)

      val startTime2 = System.currentTimeMillis()
      val row = statement.executeQuery(sql)
      println("myNodeAt() SQL used: " + (System.currentTimeMillis() - startTime2) + " ms")

      if (row.next()) {
        val result = rowToNode(row, tableName, nodeSchema(tableName))
        // println("myNodeAt() finished")
        println("myNodeAt() totally used: " + (System.currentTimeMillis() - startTime1) + " ms")
        return Some(result)
      }
    }
    None
  }

  override def nodes(): Iterator[MyNode] = {
    println("nodes()")
    val allNodes = for (tableName <- nodeSchema.keys) yield {
      val statement = connection.createStatement
      val data = statement.executeQuery(s"select * from $tableName")
      Iterator.continually(data).takeWhile(_.next())
        .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }
    }
    println("nodes() finished")
    allNodes.flatten.iterator
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[MyNode] = {
    println("nodes(nodeFilter)")
    // val startTime1 = System.currentTimeMillis()
    if (nodeFilter.labels.isEmpty && nodeFilter.properties.isEmpty) {
      return nodes()
    }

    val tableName = nodeFilter.labels.head.toString()
    val filter = nodeFilter.properties

    var sql = "select * from " + tableName
    val conditions = filter.map { case (key, value) => ("`" + key.toString() + "`", value match {
      case o => o.toString
    })
    }.map { case (key, value) => key + s" = '$value'" }.toArray
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
    // val metadata = data.getMetaData

    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }
    //      .map{ resultSet => rowToNode(resultSet, metadata)}
    // println("nodes(nodeFilter) totally used: " + (System.currentTimeMillis() - startTime2) + " ms")
    result
  }

  override def relationships(): Iterator[PathTriple] = {
    println("relationships()")
    val allRels = for (tableName <- relSchema.keys) yield {
      val statement = connection.createStatement
      val data = statement.executeQuery(s"select * from $tableName")
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
    val tableName = relationshipFilter.types.head.toString()
    val filter = relationshipFilter.properties

    var sql = "select * from " + tableName
    val conditions = filter.map { case (key, value) => ("`" + key.toString() + "`", value.value) }
      .map { case (key, value) => key + s" = '$value'" }.toArray

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
    // val metadata = data.getMetaData

    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet =>
        val startNode = myNodeAt(MyId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
        val endNode = myNodeAt(MyId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
        val rel = rowToRel(resultSet, tableName, relSchema(tableName))

        PathTriple(startNode, rel, endNode)
      }
    //      .map{ resultSet => rowToNode(resultSet, metadata)}

    println("relationships(relationshipFilter) finished")
    result
  }

  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    println("expand()")
    if (direction == BOTH) {
      return expand(id, filter, OUTGOING) ++ expand(id, filter, INCOMING)
    }

    val tableName = filter.types.head.toString()
    val startNode = direction match {
      case OUTGOING => myNodeAt(id, relMapping(tableName)._1)
      case INCOMING => myNodeAt(id, relMapping(tableName)._2)
    }
    if (startNode.isEmpty) {
      return Iterator.empty
    }

    val relType = filter.types.head.toString()
    var sql = s"select * from $relType"
    direction match {
      case OUTGOING => sql = sql + s" where $relType.`:START_ID` = ${id.toLynxInteger.value} "
      case INCOMING => sql = sql + s" where $relType.`:END_ID` = ${id.toLynxInteger.value} "
    }

    val conditions = filter.properties.map { case (key, value) => ("`" + key.toString() + "`", value.value) }
      .map { case (key, value) => s"$tableName." + key + s" = '$value'" }.toArray

    for (i <- conditions.indices) {
        sql = sql + " and " + conditions(i)
    }

    println(sql)

    val statement = connection.createStatement
    val startTime1 = System.currentTimeMillis()
    val data = statement.executeQuery(sql)
    println("expand() SQL used: " + (System.currentTimeMillis() - startTime1) + " ms")
    // val metadata = data.getMetaData

    val result = Iterator.continually(data).takeWhile(_.next())
    .map { resultSet =>
      val endNode = direction match {
        case OUTGOING => myNodeAt(MyId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
        case INCOMING => myNodeAt(MyId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
      }

      PathTriple(startNode.get, rowToRel(resultSet, relType, relSchema(relType)), endNode)
    }

    // println("expand() finished")
    println("expand() totally used: " + (System.currentTimeMillis() - startTime1) + " ms")
    result
  }

  override def expand(nodeId: LynxId, filter: RelationshipFilter,
                      endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    // println("expand(endNodeFilter)")
    val result = expand(nodeId, filter, direction).filter { pathTriple =>
      endNodeFilter.matches(pathTriple.endNode)
    }
    // println("expand(endNodeFilter) finished")
    result
  }

  //写成带有JOIN的SQL语句的paths()
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

    val relType = relationshipFilter.types.head.toString()
    val startTables = if (startNodeFilter.labels.nonEmpty) {startNodeFilter.labels.map(_.toString).toArray}
                      else {relMapping(relType)._1}
    val endTables = if (endNodeFilter.labels.nonEmpty) {endNodeFilter.labels.map(_.toString).toArray}
                    else {relMapping(relType)._2}
    //val startTable = startNodeFilter.labels(0).toString()
    //val endTable = endNodeFilter.labels(0).toString()
    val conditions = Array.concat(
      startNodeFilter.properties.map { case (key, value) => s"t1.`${key.toString()}` = '${value.toString}'" }.toArray,
      relationshipFilter.properties.map { case (key, value) => s"$relType.`${key.toString()}` = '${value.toString}'" }.toArray,
      endNodeFilter.properties.map { case (key, value) => s"t2.`${key.toString()}` = '${value.toString}'" }.toArray
    )

    val finalResult = for {
      startTable: String <- startTables
      endTable: String <- endTables
    } yield {
      var sql = s"select * from $startTable as t1 join $relType on t1.`id:ID` = "
      direction match {
        case OUTGOING => sql = sql + s"$relType.`:START_ID` join $endTable as t2 on t2.`id:ID` = $relType.`:END_ID`"
        case INCOMING => sql = sql + s"$relType.`:END_ID` join $endTable as t2 on t2.`id:ID` = $relType.`:START_ID`"
      }

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
      println("paths() combined SQL used: " + (System.currentTimeMillis() - startTime1) + " ms")

      Iterator.continually(data).takeWhile(_.next()).map{ resultSet =>
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

 //原来的paths()
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


