package org.example

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}

import java.sql.{Connection, DriverManager}
import scala.collection.mutable

class MySQLTestBase extends MyGraph {
  def loadMySQL(): Unit = {
    val url = "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "Hc1478963!"
    // var connection: Connection = _

    //val nodes: Array[String] = Array("Comment", "Forum", "Organisation", "Person", "Place", "Post", "Tag", "Tagclass")
    val nodes: Array[String] = Array("Person")
    val relations: Array[String] = Array("containerOf", "hasCreator", "hasInterest", "hasMember", "hasModerator", "hasTag",
      "hasType, isLocatedIn, isPartOf, isSubclassOf", "knows", "likes", "replyOf", "studyAt", "workAt")

    Class.forName(driver)

    val connection: Connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement

    for (nodesTable: String <- nodes) {
//        val query_1 = s"DESCRIBE ${nodesTable}"
//        val description = statement.executeQuery(query_1)
//        while (description.next) {}

      //Import nodes
      val data = statement.executeQuery(s"SELECT * FROM $nodesTable LIMIT 20000")
      val metadata = data.getMetaData
      val columnCount = metadata.getColumnCount

      while (data.next) {
        var id: Long = 0
        var labels: String = ""
        val propMap: mutable.Map[LynxPropertyKey, LynxValue] = mutable.Map.empty[LynxPropertyKey, LynxValue]

        for (i <- 1 to columnCount) {
          val columnName = metadata.getColumnName(i)
          val columnValue = data.getString(i)
          if (columnName.contains("ID")) {
            id = columnValue.toLong
          }
          else if (columnName.contains("LABEL")) {
            labels = columnValue
          }
          else {
            propMap.put(LynxPropertyKey(columnName), LynxString(columnValue))
          }
        }

        _nodes += MyId(id) -> MyNode(MyId(id), Seq(LynxNodeLabel(labels)), propMap.toMap)
      }
    }

    //Import relations
    for (nodesTable <- relations) {
      val data = statement.executeQuery(s"SELECT * FROM $nodesTable LIMIT 1000")
      val metadata = data.getMetaData
      val columnCount = metadata.getColumnCount

      while (data.next) {
        var id: Long = 0
        var relType: Option[LynxRelationshipType] = None
        var startID: Long = 0
        var endID: Long = 0
        val propMap: mutable.Map[LynxPropertyKey, LynxValue] = mutable.Map.empty

        for (i <- 1 to columnCount) {
          val columnName = metadata.getColumnName(i)
          val columnValue = data.getString(i)
          if (columnName.contains("REL_ID")) {
            id = columnValue.toLong
          }
          else if (columnName.contains("TYPE")) {
            relType = Some(LynxRelationshipType(columnValue))
          }
          else if (columnName.contains("START_ID")) {
            startID = columnValue.toLong
          }
          else if (columnName.contains("END_ID")) {
            endID = columnValue.toLong
          }
          else {
            propMap.put(LynxPropertyKey(columnName), LynxString(columnValue))
          }
        }

        _relationships += MyId(id) -> MyRelationship(MyId(id), MyId(startID), MyId(endID), relType, propMap.toMap)
      }
    }



    // this.run("match(n:Comment) set n:Message", Map())
    // this.run("match(n:Post) set n:Message", Map())
  }
}
