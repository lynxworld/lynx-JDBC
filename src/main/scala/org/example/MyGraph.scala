package org.example

import cats.effect.IO
import doobie.Fragments
import doobie.implicits.{toDoobieStreamOps, toSqlInterpolator}
import doobie._
import doobie.implicits._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorOps
import com.mysql.cj.jdbc.result.ResultSetMetaData
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import org.grapheco.lynx.{LynxRecord, LynxResult, PlanAware}
import org.grapheco.lynx.logical.{LPTNode, LogicalPlannerContext}
import org.grapheco.lynx.physical.{NodeInput, PPTNode, PhysicalPlannerContext, RelationshipInput}
import org.grapheco.lynx.runner.{CypherRunner, CypherRunnerContext, ExecutionContext, GraphModel, NodeFilter, RelationshipFilter, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationship, LynxRelationshipType, PathTriple}
import org.grapheco.lynx.types.time.LynxDate
import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.lang.NullPointerException
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date
import java.sql.{Connection, DriverManager, ResultSet}

class MyGraph extends GraphModel {

  val xa = Transactor.fromDriverManager[IO](
    "com.mysql.cj.jdbc.Driver", // driver classname
    "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false", // connect URL (driver-specific)
    "root", // user
    "Hc1478963!" // password
  )

  val url = "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "root"
  val password = "Hc1478963!"
  Class.forName(driver)

  val connection: Connection = DriverManager.getConnection(url, username, password)

  def transDate(date: Date): LocalDate = {
    val zoneid = ZoneId.systemDefault()
    LocalDate.ofEpochDay(date.getTime/86400000)
  }

  //TODO: NA value
  def rowToNode(row:  ResultSet, tableName: String, config: Array[(String, String)]): MyNode = {
    val propertyMap = (0 until config.length).map { i =>
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

  def rowToRel(row: ResultSet, relName: String, config: Array[(String, String)]): MyRelationship = {
    val propertyMap = (0 to config.length - 1).map { i =>
      val columnName = LynxPropertyKey(config(i)._1)
      val columnType = config(i)._2
      val columnValue =
        try {
          columnType match {
            case "BIGINT" => LynxValue(row.getLong(i + 1))
            case "INT" => LynxValue(row.getInt(i + 1))
            case "Date" => LynxDate(transDate(row.getDate(i + 1)))
            case "String" => LynxString(row.getString(i + 1))
            case _ => LynxString(row.getString(i + 1))
          }
        } catch {
          case ex: NullPointerException => {
            LynxNull
          }
        }
      columnName -> columnValue
    }.toMap

    val id = MyId(row.getLong("REL_ID"))
    val startId = MyId(row.getLong(":START_ID"))
    val endId = MyId(row.getLong(":END_ID"))
    MyRelationship(id, startId, endId, Some(LynxRelationshipType(relName)), propertyMap)
  }

  //Assume each table has a column "id:ID" as the primary key
  val nodeSchema = Map(
    "Person" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("creationDate", "Date"), ("firstName", "String"),
      ("lastName", "String"), ("gender", "String"), ("birthday", "Date"), ("locationIP", "String"), ("browserUsed", "String"),
      ("languages", "String"), ("emails", "String")),
    "Place" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String"), ("type", "String")),
    "Organisation"  -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("type", "String"), ("name", "String"), ("url", "String")),
    "Comment" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("creationDate", "Date"), ("locationIP", "String"),
      ("browserUsed", "String"), ("content", "String"), ("length", "INT")),
    "Post" -> Array(("creationDate", "Date"), ("id:ID", "BIGINT"), (":LABEL", "String"), ("imageFile", "String"),
      ("locationIP", "String"), ("browserUsed", "String"), ("language", "String"), ("content", "String"), ("length", "INT")),
    "Forum" -> Array(("creationDate", "Date"), ("id:ID", "BIGINT"), (":LABEL", "String"), ("title", "String")),
    "Tag" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String")),
    "Tagclass" -> Array(("id:ID", "BIGINT"), (":LABEL", "String"), ("name", "String"), ("url", "String"))
  )

  //Assume each table has a column "REL_ID" as the primary key, a column ":START_ID" as the start of rel, a column ":END_ID" as the end of rel
  val relSchema = Map(
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

  override def nodeAt(id: LynxId): Option[MyNode] = {
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
  }

  override def nodes(): Iterator[MyNode] = {
    println("nodes()")
    val allNodes = for (tableName <- nodeSchema.keys) yield {
      val statement = connection.createStatement
      val data = statement.executeQuery(s"select * from ${tableName}")
      Iterator.continually(data).takeWhile(_.next())
        .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }
    }
    println("nodes() finished")
    allNodes.flatten.iterator
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[MyNode] = {
    println("nodes(nodeFilter)")
    if (nodeFilter.labels.size == 0 && nodeFilter.properties.size == 0) {
      return nodes()
    }
  //TODO: 是否需要考虑 (p {`id:ID`: $personId} )的情况 (label长度为0但properties长度不为0)
    val tableName = nodeFilter.labels(0).toString()
    val filter = nodeFilter.properties

    var sql = "select * from " + tableName
    val conditions = filter.map { case (key, value) => ("`" + key.toString() + "`", value match {
      case o => o.toString
    })
    }.map { case (key, value) => key + s" = '${value}'" }.toArray
    for (i <- 0 until conditions.length) {
      if (i == 0) {
        sql = sql + " where " + conditions(i)
      } else {
        sql = sql + " and " + conditions(i)
      }
    }
    println(sql)
    val statement = connection.createStatement
    val data = statement.executeQuery(sql)
    // val metadata = data.getMetaData

    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }
    //      .map{ resultSet => rowToNode(resultSet, metadata)}
    println("nodes(nodeFilter) finished")
    result
  }

  override def relationships(): Iterator[PathTriple] = {
    println("relationships()")
    val allRels = for (tableName <- relSchema.keys) yield {
      val statement = connection.createStatement
      val data = statement.executeQuery(s"select * from ${tableName}")
      Iterator.continually(data).takeWhile(_.next())
        .map { resultSet =>
          val startNode = nodeAt(MyId(resultSet.getLong(":START_ID"))).get
          val endNode = nodeAt(MyId(resultSet.getLong(":END_ID"))).get
          val rel = rowToRel(resultSet, tableName, relSchema(tableName))

          PathTriple(startNode, rel, endNode)
        }
    }
    println("relationships() finished")
    allRels.flatten.iterator
  }

  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    println("relationships(relationshipFilter)")
    val tableName = relationshipFilter.types(0).toString()
    val filter = relationshipFilter.properties

    var sql = "select * from " + tableName
    val conditions = filter.map { case (key, value) => ("`" + key.toString() + "`", value.value) }
      .map { case (key, value) => key + s" = '${value}'" }.toArray

    for (i <- 0 until conditions.length) {
      if (i == 0) {
        sql = sql + " where " + conditions(i)
      } else {
        sql = sql + " and " + conditions(i)
      }
    }

    println(sql)
    val statement = connection.createStatement
    val data = statement.executeQuery(sql)
    // val metadata = data.getMetaData

    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet =>
        val startNode = nodeAt(MyId(resultSet.getLong(":START_ID"))).get
        val endNode = nodeAt(MyId(resultSet.getLong(":END_ID"))).get
        val rel = rowToRel(resultSet, tableName, relSchema(tableName))

        PathTriple(startNode, rel, endNode)
      }
    //      .map{ resultSet => rowToNode(resultSet, metadata)}

    println("relationships(relationshipFilter) finished")
    result
  }

  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    println("expand()")
    //TODO: 求证这个if是否正确
    if (direction == BOTH) {
      return expand(id, filter, OUTGOING) ++ expand(id, filter, INCOMING)
    }

    val tableName = filter.types(0).toString()
    val startNode = nodeAt(id).get
    // val startlLabel = startNode.labels
    val relType = filter.types(0).toString()
    var sql = s"select * from ${relType}"
    direction match {
      case OUTGOING => sql = sql + s" where ${relType}.`:START_ID` = ${id.toLynxInteger.value} "
      case INCOMING => sql = sql + s" where ${relType}.`:END_ID` = ${id.toLynxInteger.value} "
    }

    val conditions = filter.properties.map { case (key, value) => ("`" + key.toString() + "`", value.value) }
      .map { case (key, value) => s"${tableName}." + key + s" = '${value}'" }.toArray

    for (i <- 0 until conditions.length) {
        sql = sql + " and " + conditions(i)
    }
    println(sql)
    val statement = connection.createStatement
    val data = statement.executeQuery(sql)
    // val metadata = data.getMetaData

    val result = Iterator.continually(data).takeWhile(_.next())
    .map { resultSet =>
      val endNode = direction match {
        case OUTGOING => nodeAt(MyId(resultSet.getLong(":END_ID"))).get
        case INCOMING => nodeAt(MyId(resultSet.getLong(":START_ID"))).get
      }

      PathTriple(startNode, rowToRel(resultSet, relType, relSchema(relType)), endNode)
    }
    print("expand() finished")
    result
  }

  override def expand(nodeId: LynxId, filter: RelationshipFilter,
                      endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    println("expand(endNodeFilter)")
    val result = expand(nodeId, filter, direction).filter { pathTriple =>
      endNodeFilter.matches(pathTriple.endNode)
    }
    println("expand(endNodeFilter) finished")
    result
  }

  private val runner = new CypherRunner(this)

  def run(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = runner.run(query, param)

}
