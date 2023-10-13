package org.grapheco

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural._

case class LynxIntegerID(value: Long) extends LynxId {
  override def toLynxInteger: LynxInteger = LynxInteger(value)
}

case class LynxJDBCNode(id: LynxIntegerID, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]) extends LynxNode {
  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

  override def keys: Seq[LynxPropertyKey] = props.keys.toSeq
}

case class LynxJDBCRelationship(id: LynxIntegerID,
                                startNodeId: LynxIntegerID,
                                endNodeId: LynxIntegerID,
                                relationType: Option[LynxRelationshipType],
                                props: Map[LynxPropertyKey, LynxValue]) extends LynxRelationship {
  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

  override def keys: Seq[LynxPropertyKey] = props.keys.toSeq
}
