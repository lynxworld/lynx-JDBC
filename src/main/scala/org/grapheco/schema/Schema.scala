package org.grapheco.schema


case class Schema(nodeSchema: Seq[NodeStructure], relSchema: Seq[RelStructure], relMapping: Map[String, RelMap])
