// IS4. Content of a message
/*
:param messageId: 206158431836
 */
MATCH (m:Comment {`id:ID`:  $messageId })
RETURN
    m.creationDate as messageCreationDate,
    coalesce(m.content, m.creationDate) as messageContent
