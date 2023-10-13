package org.grapheco

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural._

case class ElementId(value: Long) extends LynxId {
  override def toLynxInteger: LynxInteger = LynxInteger(value)
}

case class ElementNode(id: ElementId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]) extends LynxNode {
  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

  override def keys: Seq[LynxPropertyKey] = props.keys.toSeq
}

case class ElementRelationship(id: ElementId,
                               startNodeId: ElementId,
                               endNodeId: ElementId,
                               relationType: Option[LynxRelationshipType],
                               props: Map[LynxPropertyKey, LynxValue]) extends LynxRelationship {
  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

  override def keys: Seq[LynxPropertyKey] = props.keys.toSeq
}
