package storage

// registerNodeSchemaIndexes inserts a committed node into every declared
// in-memory property and composite index that applies to its labels.
func (b *BadgerEngine) registerNodeSchemaIndexes(schema *SchemaManager, node *Node) error {
	if schema == nil || node == nil {
		return nil
	}
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			if _, ok := schema.GetPropertyIndex(label, propName); ok {
				if err := schema.PropertyIndexInsert(label, propName, node.ID, propValue); err != nil {
					return err
				}
			}
		}
		for _, idx := range schema.GetCompositeIndexesForLabel(label) {
			if idx == nil {
				continue
			}
			if err := idx.IndexNode(node.ID, node.Properties); err != nil {
				return err
			}
		}
	}
	return nil
}

// unregisterNodeSchemaIndexes removes a committed node from declared indexes.
// It is safe to call for non-indexed properties and labels.
func (b *BadgerEngine) unregisterNodeSchemaIndexes(schema *SchemaManager, node *Node) error {
	if schema == nil || node == nil {
		return nil
	}
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			if err := schema.PropertyIndexDelete(label, propName, node.ID, propValue); err != nil {
				return err
			}
		}
		for _, idx := range schema.GetCompositeIndexesForLabel(label) {
			if idx != nil {
				idx.RemoveNode(node.ID, node.Properties)
			}
		}
	}
	return nil
}

// replaceNodeSchemaIndexes swaps old index entries for new ones after a
// successful node update, avoiding duplicate entries for unchanged values.
func (b *BadgerEngine) replaceNodeSchemaIndexes(schema *SchemaManager, oldNode, newNode *Node) error {
	if err := b.unregisterNodeSchemaIndexes(schema, oldNode); err != nil {
		return err
	}
	return b.registerNodeSchemaIndexes(schema, newNode)
}
