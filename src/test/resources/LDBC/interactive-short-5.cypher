// IS5. Creator of a message
/*
:param messageId: 206158431836
 */
MATCH (m:Comment {id:  $messageId })-[:hasCreator]->(p:Person)
RETURN
    p.`id:ID` AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName
