// Q1. Transitive friends with certain name
/*
:param [{ personId, firstName }] => { RETURN
  4398046511333 AS personId,
  "Jose" AS firstName
}
*/
MATCH (p:Person {`id:ID`: $personId}), (friend:Person {firstName: $firstName})
       WHERE NOT p=friend
       WITH p, friend
       MATCH path = shortestPath((p)-[:knows*1..3]-(friend))
       WITH min(length(path)) AS distance, friend
ORDER BY
    distance ASC,
    friend.lastName ASC,
    toInteger(friend.`id:ID`) ASC
LIMIT 20

MATCH (friend)-[:isLocatedIn]->(friendCity:Place)
OPTIONAL MATCH (friend)-[studyAt:studyAt]->(uni:Place)-[:isLocatedIn]->(uniCity:Place)
WITH friend, collect(
    CASE uni.name
        WHEN null THEN null
        ELSE [uni.name, studyAt.classYear, uniCity.name]
    END ) AS unis, friendCity, distance

OPTIONAL MATCH (friend)-[workAt:workAt]->(company:Organisation)-[:isLocatedIn]->(companyCountry:Place)
WITH friend, collect(
    CASE company.name
        WHEN null THEN null
        ELSE [company.name, workAt.workFrom, companyCountry.name]
    END ) AS companies, unis, friendCity, distance

RETURN
    friend.`id:ID` AS friendId,
    friend.lastName AS friendLastName,
    distance AS distanceFromPerson,
    friend.birthday AS friendBirthday,
    friend.creationDate AS friendCreationDate,
    friend.gender AS friendGender,
    friend.browserUsed AS friendBrowserUsed,
    friend.locationIP AS friendLocationIp,
    friend.email AS friendEmails,
    friend.speaks AS friendLanguages,
    friendCity.name AS friendCityName,
    unis AS friendUniversities,
    companies AS friendCompanies
ORDER BY
    distanceFromPerson ASC,
    friendLastName ASC,
    toInteger(friendId) ASC
LIMIT 20
