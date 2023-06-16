// Q3. Friends and friends of friends that have been to given countries
/*
:param [{ personId, countryXName, countryYName, startDate, endDate }] => { RETURN
  6597069766734 AS personId,
  "Angola" AS countryXName,
  "Colombia" AS countryYName,
  1275393600000 AS startDate,
  1277812800000 AS endDate
}
*/
MATCH (countryX:Place {name: $countryXName }),
      (countryY:Place {name: $countryYName }),
      (person:Person {`id:ID`: $personId })
WITH person, countryX, countryY
LIMIT 1
MATCH (city:Place {`type`: City}) -[:isPartOf]-> (country:Place {`type`: Country})
WHERE country IN [countryX, countryY]
WITH person, countryX, countryY, collect(city) AS cities
MATCH (person)-[:knows*1..2]-(friend)-[:isLocatedIn]->(city)
WHERE NOT person=friend AND NOT city IN cities
WITH DISTINCT friend, countryX, countryY
MATCH (friend)<-[:hasCreator]-(message),
      (message)-[:isLocatedIn]->(country)
WHERE $endDate > message.creationDate >= $startDate AND
      country IN [countryX, countryY]
WITH friend,
     CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount>0 AND yCount>0
RETURN friend.`id:ID` AS friendId,
       friend.firstName AS friendFirstName,
       friend.lastName AS friendLastName,
       xCount,
       yCount,
       xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20
