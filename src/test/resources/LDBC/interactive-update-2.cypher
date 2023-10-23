//
//MATCH (person:Person {id: $personId}), (post:Post {id: $postId})
//CREATE (person)-[:LIKES {creationDate: $creationDate}]->(post)


//MATCH (person:Person {id: $personId})
//SET person.firstName = $firstName
//RETURN person


//MATCH (person:Person {id: $personId})
//SET person.firstName = $firstName
//RETURN person

//MATCH (n:Person {id: $personId})-[work:workAt]->(p:Organisation)
//SET work
//RETURN n, p, work

MATCH (n:Person {id: $personId})-[work:workAt]->(p:Organisation)
SET work.workFrom = $newFrom
RETURN work
