// IS1. Profile of a person
/*
:param personId: 10995116277794
 */
//MATCH (n:person {`id:ID`: $personId }) -[:isLocatedIn]-> (p:place) <-[:isLocatedIn]- (n2:person)
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

MATCH (p:place {`id:ID`: $placeId}) <-[:isLocatedIn]- (n:person)
RETURN
  n.`id:ID` AS ID,
  n.firstName AS firstName,
  n.lastName AS LastName

//MATCH (n:person) -[:isLocatedIn]-> (p:place {`id:ID`: $placeId})
//RETURN *

//MATCH (n:person {`id:ID`: $personId }) -[:isLocatedIn]-> (p:place)
//RETURN *

//MATCH (p1:person {`id:ID`: $personId }) <-[:knows]- (p2:person)
//RETURN
//  p2.`id:ID` AS ID,
//  p2.firstName AS firstName

