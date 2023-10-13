//package org.grapheco.schema
//
//object  Schema {
//  val nodeSchema = Map(
//    "Person" -> Array(
//      ("id", "BIGINT"),
//      ("creationDate", "Date"),
//      ("firstName", "String"),
//      ("lastName", "String"),
//      ("gender", "String"),
//      ("birthday", "Date"),
//      ("locationIP", "String"),
//      ("browserUsed", "String"),
//      ("languages", "String"),
//      ("emails", "String")),
//    "Place" -> Array(
//      ("id", "BIGINT"),
//      ("name", "String"),
//      ("url", "String"),
//      ("type", "String")),
//    "Organisation" -> Array(
//      ("id", "BIGINT"),
//      ("type", "String"),
//      ("name", "String"),
//      ("url", "String")),
//    "Comment" -> Array(
//      ("id", "BIGINT"),
//      ("creationDate", "Date"),
//      ("locationIP", "String"),
//      ("browserUsed", "String"),
//      ("content", "String"),
//      ("length", "INT")),
//    "Post" -> Array(
//      ("id", "BIGINT"),
//      ("creationDate", "Date"),
//      ("imageFile", "String"),
//      ("locationIP", "String"),
//      ("browserUsed", "String"),
//      ("language", "String"),
//      ("content", "String"),
//      ("length", "INT")),
//    "Forum" -> Array(
//      ("id", "BIGINT"),
//      ("creationDate", "Date"),
//      ("title", "String")),
//    "Tag" -> Array(
//      ("id", "BIGINT"),
//      ("name", "String"),
//      ("url", "String")),
//    "Tagclass" -> Array(
//      ("id", "BIGINT"),
//      ("name", "String"),
//      ("url", "String"))
//  )
//
//  val relSchema = Map(
//    "knows" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "isLocatedIn" -> Array(("REL_ID", "BIGINT") , ("START_ID", "BIGINT"), ("END_ID", "BIGINT"), ("creationDate", "Date")),
//    "containerOf" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "hasCreator" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "hasInterest" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "hasMember" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "hasModerator" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "hasTag" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "hasType" -> Array(("REL_ID", "BIGINT") , ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "isPartOf" -> Array(("REL_ID", "BIGINT") , ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "isSubclassOf" -> Array(("REL_ID", "BIGINT") , ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "likes" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "replyOf" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT")),
//    "studyAt" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT"), ("classYear", "INT")),
//    "workAt" -> Array(("REL_ID", "BIGINT") , ("creationDate", "Date"), ("START_ID", "BIGINT"), ("END_ID", "BIGINT"), ("workFrom", "INT"))
//  )
//
//  case class RelMap(source: Seq[String], target: Seq[String])
//  val relMapping: Map[String, RelMap] = Map(
//    "isLocatedIn" ->  RelMap(Array("Person", "Comment", "Post", "Organisation"), Array("Place")),
//    "replyOf" ->      RelMap(Array("Comment"), Array("Comment", "Post")),
//    "containerOf" ->  RelMap(Array("Forum"), Array("Post")),
//    "hasCreator" ->   RelMap(Array("Comment", "Post"), Array("Person")),
//    "hasInterest" ->  RelMap(Array("Person"), Array("Tag")),
//    "workAt" ->       RelMap(Array("Person"), Array("Organisation")),
//    "hasModerator" -> RelMap(Array("Forum"), Array("Person")),
//    "hasTag" ->       RelMap(Array("Comment", "Post", "Forum"), Array("Tag")),
//    "hasType" ->      RelMap(Array("Tag"), Array("Tagclass")),
//    "isSubclassOf" -> RelMap(Array("Tagclass"), Array("Tagclass")),
//    "isPartOf" ->     RelMap(Array("Place"), Array("Place")),
//    "likes" ->        RelMap(Array("Person"), Array("Comment", "Post")),
//    "knows" ->        RelMap(Array("Person"), Array("Person")),
//    "studyAt" ->      RelMap(Array("Person"), Array("Organisation")),
//    "hasMember" ->    RelMap(Array("Forum"), Array("Person")),
//  )
//
//}
