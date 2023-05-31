package org.example

import cats.effect.IO
import doobie.Fragments
import doobie.implicits.{toDoobieStreamOps, toSqlInterpolator}
import doobie._
import doobie.implicits._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import org.grapheco.lynx.{LynxRecord, LynxResult, PlanAware}
import org.grapheco.lynx.logical.{LPTNode, LogicalPlannerContext}
import org.grapheco.lynx.physical.{NodeInput, PPTNode, PhysicalPlannerContext, RelationshipInput}
import org.grapheco.lynx.runner.{CypherRunner, CypherRunnerContext, ExecutionContext, GraphModel, NodeFilter, RelationshipFilter, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationship, LynxRelationshipType, PathTriple}
import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date


class MyGraph extends GraphModel {

  val xa = Transactor.fromDriverManager[IO](
    "com.mysql.cj.jdbc.Driver", // driver classname
    "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false", // connect URL (driver-specific)
    "root", // user
    "Hc1478963!" // password
  )

  def transDate(date: Date): LocalDate = {
    val zoneid = ZoneId.systemDefault()
    LocalDate.ofEpochDay(date.getTime/86400000)
  }

  case class Person(id: Long, label: String, creationDate: Date,
                    firstName: String, lastName: String, gender: String,
                    birthday: Date, locationIP: String, browserUsed: String,
                    languages: String, emails: String) {

    def toNode: MyNode = MyNode(MyId(id), Seq(label).map(LynxNodeLabel), Map(
      "id:ID" -> id,
      "creationDate" -> transDate(creationDate),
      "firstName"-> firstName,
      "lastName" -> lastName,
      "gender" -> gender,
      "birthday" ->  transDate(birthday),
      "locationIP" -> locationIP,
      "browserUsed" -> browserUsed,
      "languages" -> languages,
      "emails" -> emails
    ).map{ case (str, serializable) => LynxPropertyKey(str) -> LynxValue(serializable)})
  }

  case class Place(id: Long, label: String, name: String,
                   url: String, placeType: String) {

    def toNode: MyNode = MyNode(MyId(id), Seq(label).map(LynxNodeLabel), Map(
      "id:ID" -> id,
      "name" -> name,
      "url" -> url,
      "type" -> placeType,
    ).map { case (str, serializable) => LynxPropertyKey(str) -> LynxValue(serializable) })
  }

  case class Knows(id: Long, ty: String, creationDate: Date, start: Long, end: Long) {
    def toRel: MyRelationship = MyRelationship(MyId(id), MyId(start), MyId(end),
      Some(LynxRelationshipType(ty)), Map(
        "creationDate" -> transDate(creationDate)
      ).map{ case (str, serializable) => LynxPropertyKey(str) -> LynxValue(serializable)})
  }

  case class isLocatedIn(id: Long, ty: String, start: Long, end: Long, creationDate: Option[Date]) {
    def toRel: MyRelationship =
      if (creationDate.isEmpty) {
        MyRelationship(MyId(id), MyId(start), MyId(end), Some(LynxRelationshipType(ty)), Map())
      } else {
        MyRelationship(MyId(id), MyId(start), MyId(end),
          Some(LynxRelationshipType(ty)), Map(
            "creationDate" -> transDate(creationDate.get)
          ).map { case (str, serializable) => LynxPropertyKey(str) -> LynxValue(serializable) })
      }
  }

  def getNodeFromTable(tableName: String, filter: Map[LynxPropertyKey, LynxValue]): Iterator[MyNode] = {
    val head = fr"select * from " ++ Fragment.const(tableName)
    val frs = filter.map{ case (key, value) => ("`" + key.toString() + "`", value match {
      //TODO: Handle other cases such as Int
      case o => o.toString
    })}.map{ case (key, value) => Fragment.const(key) ++ fr"=$value"}.toSeq
    val sql = head ++ Fragments.whereAnd(frs:_*)
    //    println(sql)
    tableName match {
      case "Person" => sql.query[Person].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toNode)
      case "Place" => sql.query[Place].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toNode)
    }
  }

  def getRelationFromTable(tableName: String, filter: Map[LynxPropertyKey, LynxValue]): Iterator[PathTriple] = {
    val head = fr"select * from " ++ Fragment.const(tableName)
    val frs = filter.map{ case (key, value) => ("`" + key.toString() + "`", value match {
      //TODO: Handle other cases such as Int
      case o => o.toString
    })}.map{case (key, value) => Fragment.const(key) ++ fr"=$value"}.toSeq
    val sql = head ++ Fragments.whereAnd(frs:_*)
    tableName match {
      case "knows" => sql.query[Knows].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toRel)
        .map(rel => PathTriple(nodeAt(rel.startNodeId).get, rel, nodeAt(rel.endNodeId).get))
      case "isLocatedIn" => sql.query[isLocatedIn].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toRel)
        .map(rel => PathTriple(nodeAt(rel.startNodeId).get, rel, nodeAt(rel.endNodeId).get))
    }
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

    override def commit: Boolean = {true}
  }

  override def nodeAt(id: LynxId): Option[MyNode] = {
    id.toString().charAt(0) match {
      case '2' =>
        sql"select * from Place where `id:ID` = ${id.toLynxInteger.value}"
          .query[Place].option.transact(xa).unsafeRunSync().map(_.toNode)
      case '3' =>
        sql"select * from Person where `id:ID` = ${id.toLynxInteger.value}"
          .query[Person].option.transact(xa).unsafeRunSync().map(_.toNode)
      case _ => throw new RuntimeException("node table not implemented")
    }
//    sql"select * from Person where `id:ID` = 300000000008100"
//      .query[Person].option.transact(xa).unsafeRunSync().map(_.toNode)
  }

  override def nodes(): Iterator[MyNode] = {
    val node1 = sql"SELECT * FROM Person"
      .query[Person].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toNode)
    val node2 = sql"SELECT * FROM Place"
      .query[Place].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toNode)

    node1 ++ node2
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[MyNode] = nodeFilter.labels match {
    case Seq(LynxNodeLabel("person")) => getNodeFromTable("Person", nodeFilter.properties)
    case Seq(LynxNodeLabel("place")) => getNodeFromTable("Place", nodeFilter.properties)
    case _ => throw new RuntimeException("...")
  }

  override def relationships(): Iterator[PathTriple] = {
    val rel1 = sql"SELECT * FROM knows"
      .query[Knows].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toRel)
      .map(rel => PathTriple(nodeAt(rel.startNodeId).get, rel, nodeAt(rel.endNodeId).get))

    val rel2 = sql"SELECT * FROM isLocatedIn"
      .query[isLocatedIn].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toRel)
      .map(rel => PathTriple(nodeAt(rel.startNodeId).get, rel, nodeAt(rel.endNodeId).get))

    rel1 ++ rel2
  }

  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    val relType: String = relationshipFilter.types(0).toString()
    getRelationFromTable(relType, relationshipFilter.properties)
  }

  //TODO: Revise this to adapt other relationships
  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = filter.types match {
    case Seq(LynxRelationshipType("knows")) => {
      //      println(s"SELECT * FROM Person,knows WHERE Person.id = knows.`:END_ID` and knows.`:START_ID` = ${id.toLynxInteger.value}")
      nodeAt(id).map{ start =>
        val out = fr"Person.`id:ID` = knows.`:END_ID` and knows.`:START_ID` = ${id.toLynxInteger.value}"
        val in  = fr"Person.`id:ID` = knows.`:START_ID` and knows.`:END_ID` = ${id.toLynxInteger.value}"
        val sql = fr"SELECT * FROM Person,knows  " ++ (direction match {
          case OUTGOING => Fragments.whereAnd(out)
          case INCOMING => Fragments.whereAnd(in)
          case BOTH => Fragments.whereOr(out,in)
        })
        //        println(sql)
        sql.query[(Person, Knows)].to[List].transact(xa).unsafeRunSync().toIterator
          .map{ case (person, knows) => PathTriple(start, knows.toRel, person.toNode)}
      }.getOrElse(Iterator.empty)
    }

    case Seq(LynxRelationshipType("isLocatedIn")) => {
      nodeAt(id).map{ current =>
        val out = fr"Place.`id:ID` = isLocatedIn.`:END_ID` and isLocatedIn.`:START_ID` = ${id.toLynxInteger.value}"
        val in = fr"Person.`id:ID` = isLocatedIn.`:START_ID` and isLocatedIn.`:END_ID` = ${id.toLynxInteger.value}"
        direction match {
          //current is a person
          case OUTGOING =>
            val sql = fr"SELECT * FROM Place, isLocatedIn" ++ Fragments.whereAnd(out)
            sql.query[(Place, isLocatedIn)].to[List].transact(xa).unsafeRunSync().toIterator
              .map { case (place, islocatedin) => PathTriple(current, islocatedin.toRel, place.toNode) }
          //current is a place
          case INCOMING =>
            val sql = fr"SELECT * FROM Person, isLocatedIn" ++ Fragments.whereAnd(in)
            sql.query[(Person, isLocatedIn)].to[List].transact(xa).unsafeRunSync().toIterator
              .map { case (person, isLocatedIn) => PathTriple(current, isLocatedIn.toRel, person.toNode) }
          case BOTH => throw new RuntimeException("both not implemented")
        }
      }.getOrElse(Iterator.empty)
    }

    case _ => throw new RuntimeException("..")
  }

  private val runner = new CypherRunner(this)

  def run(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = runner.run(query, param)

}
