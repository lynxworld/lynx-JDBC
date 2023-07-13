// IS2. Recent messages of a person
/*
:param personId: 10995116277795····
 */

//测试用
//(p2) <- ... - (p:Post) 调用了非常多次expand，然后再join，导致时间非常慢

//MATCH (p1:Person {`id:ID`: $personId }) -[:knows]-> (p2:Person)
//  WITH p2
//MATCH (p2) <-[:hasCreator]- (p:Post)
//RETURN
//  p2.`id:ID` AS ID,
//  p2.firstName AS firstName,
//  p2.lastName AS lastName,
//  p.id AS postID


//原Cypher语句

MATCH (:Person {`id:ID`: $personId})<-[:hasCreator]-(message)
WITH
  message,
  message.`id:ID` AS messageId,
  message.creationDate AS messageCreationDate
  ORDER BY messageCreationDate DESC, messageId ASC
  LIMIT 1
MATCH (message)-[:replyOf*0..]->(post:Post)-[:hasCreator]->(person:Person)
RETURN
  messageId,
  coalesce(message.content,message.creationDate) AS messageContent,
  messageCreationDate,
  post.`id:ID` AS postId,
  person.`id:ID` AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName
   ORDER BY messageCreationDate DESC, messageId ASC