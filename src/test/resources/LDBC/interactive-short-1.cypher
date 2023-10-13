// IS1. Profile of a person
/*
:param personId: 10995116277794
 */

MATCH (n:Person {id: $personId }) -[:workAt]-> (p:Organisation)
RETURN
  n.lastName AS name,
  p.name AS Org_name

//MATCH (n:Person {id: $personId }) -[:isLocatedIn]-> (p:Place)
//RETURN
//    n.firstName AS firstName,
//    n.lastName AS lastName,
//    n.birthday AS birthday,
//    n.locationIP AS locationIP,
//    n.browserUsed AS browserUsed,
//    p.`id:ID` AS cityId,
//    n.gender AS gender,
//    n.creationDate AS creationDate

//MATCH (n:Person {`id:ID`: $personId }) -[:isLocatedIn]-> (p:Place) <-[:isLocatedIn]- (n2:Person)
//RETURN
//    n2.firstName AS firstName,
//    n2.lastName AS lastName,
//    n2.birthday AS birthday,
//    n2.locationIP AS locationIP,
//    n2.browserUsed AS browserUsed,
//    p.`id:ID` AS placeId,
//    p.name AS placeName,
//    n2.gender AS gender,
//    n2.creationDate AS creationDate

//MATCH (p:Place {`id:ID`: $placeId}) <-[:isLocatedIn]- (n:Person)
//RETURN
//  n.`id:ID` AS ID,
//  n.firstName AS firstName,
//  n.lastName AS LastName

//MATCH (n:Person) -[:isLocatedIn]-> (p:Place {`id:ID`: $placeId})
//RETURN *

//MATCH (n:Person {`id:ID`: $personId }) -[:isLocatedIn]-> (p:Place)
//RETURN
//  n.`id:ID` as personId,
//  n.firstName as firstName,
//  n.lastName as lastName,
//  p.`id:ID` as placeId,
//  p.name as placeName

//MATCH (p1:Person {`id:ID`: $personId }) <-[:knows]- (p2:Person)
//RETURN
//  p2.`id:ID` AS ID,
//  p2.firstName AS firstName,
//  p2.lastName AS lastName,
//  p2.birthday AS birthday



