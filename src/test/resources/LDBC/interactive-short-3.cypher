// IS3. Friends of a person
/*
:param personId: 10995116277794
 */
MATCH (n:Person {id: $personId })-[r:knows]-(friend:Person)
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY
    friendshipCreationDate DESC,
    toInteger(personId) ASC
