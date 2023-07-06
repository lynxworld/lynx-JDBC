// Q7. Recent likers
/*
:param personId: 4398046511268
*/

//MATCH (person:Person {`id:ID`: $personId})<-[:hasCreator]-(message:Comment)<-[like:likes]-(liker:Person)
//    WITH liker, message, like.creationDate AS likeTime, person
//RETURN
//    liker.`id:ID` AS personId,
//    liker.firstName AS personFirstName,
//    liker.lastName AS personLastName,
//    likeTime AS likeCreationDate,
//    message.`id:ID` AS commentOrPostId,
//    coalesce(message.content, message.creationDate) AS commentOrPostContent,
//    // toInteger(floor(toFloat(likeTime - message.creationDate)/1000.0)/60.0) AS minutesLatency,
//    not((liker)-[:knows]-(person)) AS isNew
//LIMIT 20



MATCH (person:Person {`id:ID`: $personId})<-[:hasCreator]-(message:Comment)<-[like:likes]-(liker:Person)
     WITH liker, message, like.creationDate AS likeTime, person
     ORDER BY likeTime DESC, toInteger(message.`id:ID`) ASC
     WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
RETURN
    liker.`id:ID` AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    latestLike.likeTime AS likeCreationDate,
    latestLike.msg.`id:ID` AS commentOrPostId,
    coalesce(latestLike.msg.content, latestLike.msg.creationDate) AS commentOrPostContent,
    toInteger(floor(toFloat(latestLike.likeTime - latestLike.msg.creationDate)/1000.0)/60.0) AS minutesLatency,
    not((liker)-[:knows]-(person)) AS isNew
ORDER BY
    likeCreationDate DESC,
    toInteger(personId) ASC
LIMIT 20
