/*
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
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType, PathTriple}
import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date

class Demo extends GraphModel {

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
      "id" -> id,
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

  case class Knows(id: Long, ty: String, creationDate: Date, start: Long, end: Long) {
    def toRel: MyRelationship = MyRelationship(MyId(id), MyId(start), MyId(end),
      Some(LynxRelationshipType(ty)), Map(
      "creationDate" -> transDate(creationDate)
    ).map{ case (str, serializable) => LynxPropertyKey(str) -> LynxValue(serializable)})
  }

  def getNodeFromTable(tableName: String, filter: Map[LynxPropertyKey, LynxValue]): Iterator[MyNode] = {
    val head = fr"select * from " ++ Fragment.const(tableName)
    val frs = filter.map{ case (key, value) => (key.toString(), value match {
//      case LynxInteger(v) => v
      case o => o.toString
    })}.map{ case (key, value) => Fragment.const(key) ++ fr"=$value"}.toSeq
    val sql = head ++ Fragments.whereAnd(frs:_*)
//    println(sql)
    sql.query[Person].to[List].transact(xa).unsafeRunSync().toIterator.map(_.toNode)
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
//    println(s"select * from Person where id = ${id}")
    sql"select * from Person where id = ${id.toLynxInteger.value}"
      .query[Person].option.transact(xa).unsafeRunSync().map(_.toNode)
  }

  override def nodes(): Iterator[MyNode] = ???

  override def nodes(nodeFilter: NodeFilter): Iterator[MyNode] = nodeFilter.labels match {
    case Seq(LynxNodeLabel("Person")) => getNodeFromTable("Person", nodeFilter.properties)
    case _ => throw new RuntimeException("...")
  }

  override def relationships(): Iterator[PathTriple] = ???

//  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = relationshipFilter.types match {
//    case Seq(LynxRelationshipType("KNOWS")) =>
//    case _ => throw new RuntimeException("..")
//  }

  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = filter.types match {
    case Seq(LynxRelationshipType("KNOWS")) => {
//      println(s"SELECT * FROM Person,knows WHERE Person.id = knows.`:END_ID` and knows.`:START_ID` = ${id.toLynxInteger.value}")
      nodeAt(id).map{ start =>
        val out = fr"Person.id = knows.`:END_ID` and knows.`:START_ID` = ${id.toLynxInteger.value}"
        val in  = fr"Person.id = knows.`:START_ID` and knows.`:END_ID` = ${id.toLynxInteger.value}"
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
    case _ => throw new RuntimeException("..")
  }


}

case class MyId(value: Long) extends LynxId {
  override def toLynxInteger: LynxInteger = LynxInteger(value)

  override def toString: String = value.toString
}

case class MyNode(id: MyId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]) extends LynxNode {
  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

  override def keys: Seq[LynxPropertyKey] = props.keys.toSeq

}

case class MyRelationship(id: MyId,
                            startNodeId: MyId,
                            endNodeId: MyId,
                            relationType: Option[LynxRelationshipType],
                            props: Map[LynxPropertyKey, LynxValue]) extends LynxRelationship {
  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

  override def keys: Seq[LynxPropertyKey] = props.keys.toSeq
}

class DemoRunner(model: GraphModel) extends CypherRunner(model: GraphModel){
  private implicit lazy val runnerContext = CypherRunnerContext(types, procedures, dataFrameOperator, expressionEvaluator, model)

  def run(query: String, param: Map[String, Any], ast: Boolean, lp: Boolean, pp: Boolean): LynxResult = {
    val (statement, param2, state) = queryParser.parse(query)
    if (ast) println(s"AST tree: ${statement}")

    val logicalPlannerContext = LogicalPlannerContext(param ++ param2, runnerContext)
    val logicalPlan = logicalPlanner.plan(statement, logicalPlannerContext)
    if (lp) println(s"logical plan: \r\n${logicalPlan.pretty}")

    val physicalPlannerContext = PhysicalPlannerContext(param ++ param2, runnerContext)
    val physicalPlan = physicalPlanner.plan(logicalPlan)(physicalPlannerContext)
    //    logger.debug(s"physical plan: \r\n${physicalPlan.pretty}")

    val optimizedPhysicalPlan = physicalPlanOptimizer.optimize(physicalPlan, physicalPlannerContext)
    if (pp) println(s"optimized physical plan: \r\n${optimizedPhysicalPlan.pretty}")

    val ctx = ExecutionContext(physicalPlannerContext, statement, param ++ param2)
    val df = optimizedPhysicalPlan.execute(ctx)

    new LynxResult() with PlanAware {
      val schema = df.schema
      val columnNames = schema.map(_._1)
      val columnMap = columns().zipWithIndex.toMap

      override def show(limit: Int): Unit =
        FormatUtils.printTable(columnNames, df.records.take(limit).toSeq.map(_.map(types.format)))

      override def columns(): Seq[String] = columnNames

      override def records(): Iterator[LynxRecord] = df.records.map(values => LynxRecord(columnMap, values))

      override def getASTStatement(): (Statement, Map[String, Any]) = (statement, param2)

      override def getLogicalPlan(): LPTNode = logicalPlan

      override def getPhysicalPlan(): PPTNode = physicalPlan

      override def getOptimizerPlan(): PPTNode = optimizedPhysicalPlan

      override def cache(): LynxResult = {
        val source = this
        val cached = df.records.toSeq

        new LynxResult {
          override def show(limit: Int): Unit =
            FormatUtils.printTable(columnNames, cached.take(limit).map(_.map(types.format)))

          override def cache(): LynxResult = this

          override def columns(): Seq[String] = columnNames

          override def records(): Iterator[LynxRecord] = cached.map(values => LynxRecord(columnMap, values)).toIterator

        }
      }
    }
  }
}*/
