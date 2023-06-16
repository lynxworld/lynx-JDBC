// Q2. Recent messages by your friends
/*
:param [{ personId, maxDate }] => { RETURN
  10995116278009 AS personId,
  1287230400000 AS maxDate
}
*/
MATCH (:Person {`id:ID`: $personId })-[:knows]-(friend:Person)<-[:hasCreator]-(message:Comment)
    WHERE message.creationDate <= $maxDate
    RETURN
        friend.`id:ID` AS personId,
        friend.firstName AS personFirstName,
        friend.lastName AS personLastName,
        message.`id:ID` AS postOrCommentId,
        coalesce(message.content,message.creationDate) AS postOrCommentContent,
        message.creationDate AS postOrCommentCreationDate
    ORDER BY
        postOrCommentCreationDate DESC,
        toInteger(postOrCommentId) ASC
    LIMIT 20
