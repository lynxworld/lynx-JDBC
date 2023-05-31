package org.example

import org.grapheco.lynx.{LynxRecord, LynxResult, PlanAware}
import org.grapheco.lynx.logical.{LPTNode, LogicalPlannerContext}
import org.grapheco.lynx.physical.{PPTNode, PhysicalPlannerContext}
import org.grapheco.lynx.runner.{CypherRunner, CypherRunnerContext, ExecutionContext, GraphModel}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.util.FormatUtils
import org.opencypher.v9_0.ast.Statement

import java.sql.{Connection, DriverManager}
import scala.collection.mutable

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

}
