// IS6. Forum of a message
/*
:param messageId: 206158431836
 */
MATCH (m:Comment {`id:ID`: $messageId })-[:replyOf*0..]->(p:Post)<-[:containerOf]-(f:Forum)-[:hasModerator]->(mod:Person)
RETURN
    f.`id:ID` AS forumId,
    f.title AS forumTitle //,
//    mod.`id:ID` AS moderatorId,
//    mod.firstName AS moderatorFirstName,
//    mod.lastName AS moderatorLastName
