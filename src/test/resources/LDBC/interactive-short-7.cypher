// IS7. Replies of a message
/*
:param messageId: 206158432794
 */
MATCH (m:Comment {`id:ID`: $messageId })<-[:replyOf]-(c:Comment)-[:hasCreator]->(p:Person)
    OPTIONAL MATCH (m)-[:hasCreator]->(a:Person)-[r:knows]-(p)
    RETURN c.`id:ID` AS commentId,
        c.content AS commentContent,
        c.creationDate AS commentCreationDate,
        p.`id:ID` AS replyAuthorId,
        p.firstName AS replyAuthorFirstName,
        p.lastName AS replyAuthorLastName,
        CASE r
            WHEN null THEN false
            ELSE true
        END AS replyAuthorKnowsOriginalMessageAuthor
    ORDER BY commentCreationDate DESC, replyAuthorId
