// Q6. Tag co-occurrence
/*
:param [{ personId, tagName }] => { RETURN
  4398046511333 AS personId,
  "Carl_Gustaf_Emil_Mannerheim" AS tagName
}
*/
MATCH (knownTag:Tag { name: $tagName })
WITH knownTag.`id:ID` as knownTagId

MATCH (person:Person { `id:ID`: $personId })-[:knows*1..2]-(friend)
WHERE NOT person=friend
WITH
    knownTagId,
    collect(distinct friend) as friends
UNWIND friends as f
    MATCH (f)<-[:hasCreator]-(post:Post),
          (post)-[:hasTag]->(t:Tag{`id:ID`: knownTagId}),
          (post)-[:hasTag]->(tag:Tag)
    WHERE NOT t = tag
    WITH
        tag.name as tagName,
        count(post) as postCount
RETURN
    tagName,
    postCount
ORDER BY
    postCount DESC,
    tagName ASC
LIMIT 10
