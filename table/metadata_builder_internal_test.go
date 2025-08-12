// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func schema() iceberg.Schema {
	return *iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
		iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
}

func sortOrder() SortOrder {
	// TODO: rust has a constructor for SortOrder which checks for compat to schema
	return SortOrder{
		OrderID: 1,
		Fields: []SortField{
			{
				SourceID:  3,
				Direction: SortDESC,
				NullOrder: NullsFirst,
			},
		},
	}
}

func partitionSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpecID(0,
		iceberg.PartitionField{
			SourceID:  2,
			Name:      "y",
			Transform: iceberg.IdentityTransform{},
		},
	)
}

const (
	TestLocation         = "s3://bucket/test/location"
	LastAssignedColumnID = 3
)

func builderWithoutChanges(formatVersion int) MetadataBuilder {
	tableSchema := schema()
	partitionSpec := partitionSpec()
	sortOrder := sortOrder()
	props := make(map[string]string)
	builder, err := NewMetadataBuilderFromPieces(
		&tableSchema, partitionSpec, sortOrder, TestLocation, formatVersion, props)
	if err != nil {
		panic(err)
	}

	return *builder
}

func TestMinimalBuild(t *testing.T) {
	tableSchema := schema()
	partitionSpec := partitionSpec()
	sortOrder := sortOrder()
	formatVersion := 1
	props := make(map[string]string)
	builder, err := NewMetadataBuilderFromPieces(
		&tableSchema, partitionSpec, sortOrder, TestLocation, formatVersion, props)
	require.NoError(t, err)
	require.NotNil(t, builder)
	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.Equal(t, 1, meta.FormatVersion())
	require.Equal(t, TestLocation, meta.Location())
	require.Equal(t, 0, meta.CurrentSchema().ID)
	require.Equal(t, 0, meta.DefaultPartitionSpec())
	require.Equal(t, 1, meta.DefaultSortOrder())
	require.Equal(t, 1000, *meta.LastPartitionSpecID())
	require.Equal(t, 3, meta.LastColumnID())
	require.Equal(t, 0, len(meta.Snapshots()))
	require.Nil(t, meta.CurrentSnapshot())
	for range meta.Refs() {
		t.Fatalf("refs should be empty.")
	}
	require.Equal(t, 0, len(meta.Properties()))
	for range meta.PreviousFiles() {
		t.Fatalf("metadata log should be empty.")
	}
	require.Equal(t, int64(0), meta.LastSequenceNumber())
	require.Equal(t, LastAssignedColumnID, meta.LastColumnID())
}

func TestBuildUnpartitionedUnsorted(t *testing.T) {
	tableSchema := schema()
	builder, err := NewMetadataBuilderFromPieces(
		&tableSchema,
		*iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		TestLocation,
		2,
		map[string]string{},
	)
	require.NoError(t, err)
	require.NotNil(t, builder)
	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.Equal(t, 2, meta.FormatVersion())
	require.Equal(t, TestLocation, meta.Location())
	require.Equal(t, 0, meta.CurrentSchema().ID)
	require.Equal(t, 0, meta.DefaultPartitionSpec())
	require.Equal(t, 0, meta.DefaultSortOrder())
	require.Equal(t, 999, *meta.LastPartitionSpecID())
	require.Equal(t, 3, meta.LastColumnID())
	require.Equal(t, 0, len(meta.Snapshots()))
	require.Nil(t, meta.CurrentSnapshot())
	for range meta.Refs() {
		t.Fatalf("refs should be empty.")
	}
	require.Equal(t, len(meta.Properties()), 0)
	for range meta.PreviousFiles() {
		t.Fatalf("metadata log should be empty.")
	}
	require.Equal(t, meta.LastSequenceNumber(), int64(0))
	require.Equal(t, meta.LastColumnID(), 3)
}

// table_metadata_builder::test::test_reassigns_ids
func TestReassignIds(t *testing.T) {
	schema := iceberg.NewSchema(10, iceberg.NestedField{
		ID:       11,
		Name:     "a",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:       12,
		Name:     "b",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:   13,
		Name: "struct",
		Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{
					Type:     iceberg.PrimitiveTypes.Int64,
					ID:       14,
					Name:     "nested",
					Required: true,
				},
			},
		},
		Required: true,
	},
		iceberg.NestedField{
			ID:       15,
			Name:     "c",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		})

	spec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(20),
		iceberg.AddPartitionFieldByName("a", "a", iceberg.IdentityTransform{}, schema, nil),
		iceberg.AddPartitionFieldByName("struct.nested", "nested_partition", iceberg.IdentityTransform{}, schema, nil))

	require.NoError(t, err)

	sortOrder := SortOrder{
		OrderID: 10,
		Fields: []SortField{
			{
				SourceID:  11,
				Transform: iceberg.IdentityTransform{},
				Direction: SortASC,
				NullOrder: NullsFirst,
			},
		},
	}
	meta, err := NewMetadataBuilderFromPieces(
		schema,
		spec,
		sortOrder,
		TestLocation,
		2,
		map[string]string{})
	require.NoError(t, err)
	require.NotNil(t, meta)

	expectedSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "a",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:       2,
		Name:     "b",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:   3,
		Name: "struct",
		Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{
					Type: iceberg.PrimitiveTypes.Int64,
					// TODO: this is discrepancy with rust impl, is 5 over there
					ID:       4,
					Name:     "nested",
					Required: true,
				},
			},
		},
		Required: true,
	},
		iceberg.NestedField{
			// TODO: this is discrepancy with rust impl, is 4 over there
			ID:       5,
			Name:     "c",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		})
	id := 1000
	fieldID := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(0),
		iceberg.AddPartitionFieldByName("a", "a", iceberg.IdentityTransform{}, expectedSchema, &id),
		iceberg.AddPartitionFieldByName("struct.nested", "nested_partition", iceberg.IdentityTransform{}, expectedSchema, &fieldID))

	require.NoError(t, err)

	expectedSortOrder := SortOrder{
		OrderID: 1,
		Fields: []SortField{
			{
				SourceID:  1,
				Transform: iceberg.IdentityTransform{},
				Direction: SortASC,
				NullOrder: NullsFirst,
			},
		},
	}

	require.True(t, expectedSchema.Equals(meta.schemaList[0]), cmp.Diff(spew.Sdump(expectedSchema.Fields()), spew.Sdump(meta.schemaList[0].Fields())))
	require.True(t, expectedSpec.Equals(meta.specs[0]))
	require.True(t, expectedSortOrder.Equals(meta.sortOrderList[0]))
}

func TestAddRemovePartitionSpec(t *testing.T) {
	builder := builderWithoutChanges(2)
	builderRef := &builder
	i := 1000
	addedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(10), iceberg.AddPartitionFieldBySourceID(2, "y", iceberg.IdentityTransform{}, builder.schemaList[0], &i), iceberg.AddPartitionFieldBySourceID(3, "z", iceberg.IdentityTransform{}, builder.schemaList[0], nil))
	require.NoError(t, err)

	builderRef, err = builderRef.AddPartitionSpec(&addedSpec, false)
	require.NoError(t, err)
	metadata, err := builderRef.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)

	i2 := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(1), iceberg.AddPartitionFieldBySourceID(2, "y", iceberg.IdentityTransform{}, builder.schemaList[0], &i), iceberg.AddPartitionFieldBySourceID(3, "z", iceberg.IdentityTransform{}, builder.schemaList[0], &i2))
	require.NoError(t, err)
	require.Equal(t, metadata.DefaultPartitionSpec(), 0)
	require.Equal(t, *metadata.LastPartitionSpecID(), i2)
	found := false
	for _, part := range metadata.PartitionSpecs() {
		if part.ID() == 1 {
			found = true
			require.True(t, part.Equals(expectedSpec), "expected partition spec to match added spec")
		}
	}
	require.True(t, found, "expected partition spec to be added")

	newBuilder, err := MetadataBuilderFromBase(metadata, nil)
	require.NoError(t, err)
	// Remove the spec
	newBuilderRef, err := newBuilder.RemovePartitionSpecs([]int{1})
	require.NoError(t, err)
	newBuild, err := newBuilderRef.Build()
	require.NoError(t, err)
	require.NotNil(t, newBuild)
	require.Len(t, newBuilder.updates, 1)
	require.Len(t, newBuild.PartitionSpecs(), 1)
	_, err = newBuilder.GetSpecByID(1)
	require.ErrorContains(t, err, "partition spec with id 1 not found")
}

func TestSetDefaultPartitionSpec(t *testing.T) {
	builder := builderWithoutChanges(2)
	curSchema, err := builder.GetSchemaByID(builder.currentSchemaID)
	require.NoError(t, err)
	// Add a partition spec
	addedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(10), iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, nil))
	require.NoError(t, err)
	builderRef, err := builder.AddPartitionSpec(&addedSpec, false)
	require.NoError(t, err)
	// Set the default partition spec
	builderRef, err = builderRef.SetDefaultSpecID(-1)
	require.NoError(t, err)

	id := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(1), iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, &id))
	require.NoError(t, err)
	require.True(t, builderRef.HasChanges())
	require.Equal(t, len(builderRef.updates), 2)
	require.True(t, builderRef.updates[0].(*addPartitionSpecUpdate).Spec.Equals(addedSpec))
	require.Equal(t, -1, builderRef.updates[1].(*setDefaultSpecUpdate).SpecID)
	metadata, err := builderRef.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)

	require.Equal(t, metadata.DefaultPartitionSpec(), 1)
	require.True(t, expectedSpec.Equals(metadata.PartitionSpec()), fmt.Sprintf("expected partition spec to match added spec %s, %s", spew.Sdump(expectedSpec), spew.Sdump(metadata.PartitionSpec())))
	require.Equal(t, *metadata.LastPartitionSpecID(), 1001)
}

func TestSetExistingDefaultPartitionSpec(t *testing.T) {
	// Arrange: Create a builder, get the current schema, and add a partition spec.
	builder := builderWithoutChanges(2)
	curSchema, err := builder.GetSchemaByID(builder.currentSchemaID)
	require.NoError(t, err)

	addedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(10), iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, nil))
	require.NoError(t, err)

	builderRef, err := builder.AddPartitionSpec(&addedSpec, false)
	require.NoError(t, err)

	// Act: Set the default partition spec to an invalid ID (-1).
	builderRef, err = builderRef.SetDefaultSpecID(-1)
	require.NoError(t, err)

	// Assert: Verify the updates and build the initial metadata.
	require.True(t, builderRef.HasChanges())
	require.Len(t, builderRef.updates, 2)
	require.True(t, builderRef.updates[0].(*addPartitionSpecUpdate).Spec.Equals(addedSpec))
	require.Equal(t, -1, builderRef.updates[1].(*setDefaultSpecUpdate).SpecID)

	metadata, err := builderRef.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.Equal(t, 1, metadata.DefaultPartitionSpec())

	id := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(1), iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, &id))
	require.NoError(t, err)
	require.True(t, expectedSpec.Equals(metadata.PartitionSpec()), "expected partition spec to match added spec")

	// ---

	// Arrange: Create a new builder from the previously built metadata.
	newBuilder, err := MetadataBuilderFromBase(metadata, nil)
	require.NoError(t, err)
	require.NotNil(t, newBuilder)

	// Act: Set the default partition spec to a valid ID (0).
	newBuilderRef, err := newBuilder.SetDefaultSpecID(0)
	require.NoError(t, err)

	// Assert: Verify the new updates and the final built metadata.
	require.True(t, newBuilderRef.HasChanges(), "expected changes after setting default spec")
	require.Len(t, newBuilderRef.updates, 1, "expected one update")
	require.Equal(t, 0, newBuilderRef.updates[0].(*setDefaultSpecUpdate).SpecID, "expected default partition spec to be set to 0")

	newBuild, err := newBuilderRef.Build()
	require.NoError(t, err)
	require.NotNil(t, newBuild)
	require.Equal(t, 0, newBuild.DefaultPartitionSpec(), "expected default partition spec to be set to 0")

	newWithoutChanges := builderWithoutChanges(2)
	require.True(t, newWithoutChanges.specs[0].Equals(newBuild.PartitionSpec()), "expected partition spec to match added spec")
}

func TestAddSortOrder(t *testing.T) {
	builder := builderWithoutChanges(2)
	addedSortOrder := SortOrder{
		OrderID: 10,
		Fields: []SortField{
			{
				SourceID:  1,
				Transform: iceberg.IdentityTransform{},
				Direction: SortASC,
				NullOrder: NullsFirst,
			},
		},
	}
	s := schema()
	err := addedSortOrder.CheckCompatibility(&s)
	require.NoError(t, err)
	builderRef, err := builder.AddSortOrder(&addedSortOrder)
	require.NoError(t, err)
	require.NotNil(t, builderRef)
	expectedSortOrder := SortOrder{
		OrderID: 2,
		Fields: []SortField{
			{
				SourceID:  1,
				Transform: iceberg.IdentityTransform{},
				Direction: SortASC,
				NullOrder: NullsFirst,
			},
		},
	}
	require.True(t, builderRef.HasChanges())
	require.Equal(t, len(builderRef.updates), 1)
	i := 0
	for _, order := range builderRef.sortOrderList {
		if order.OrderID > i {
			i = order.OrderID
		}
	}
	require.Equal(t, 2, i, "expected max sort order to be 2")
	sO, err := builderRef.GetSortOrderByID(2)
	require.NoError(t, err)
	require.True(t, sO.Equals(expectedSortOrder))
	order := sortOrder()
	update := NewAddSortOrderUpdate(&order)
	require.Equal(t, update.Action(), builderRef.updates[0].Action())
}

func TestAddCompatibleSchema(t *testing.T) {
	builder := builderWithoutChanges(2)
	addedSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "x",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:       2,
		Name:     "y",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:       3,
		Name:     "z",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:       4,
		Name:     "a",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})

	builderRef, err := builder.AddSchema(addedSchema)
	require.NoError(t, err)
	require.NotNil(t, builderRef)
	_, err = builderRef.SetCurrentSchemaID(-1)
	require.NoError(t, err)
	require.True(t, builderRef.HasChanges())
	require.Equal(t, len(builderRef.updates), 2)

	require.Equal(t, 1, builderRef.currentSchemaID)
	curSchema, err := builderRef.GetSchemaByID(builderRef.currentSchemaID)
	require.NoError(t, err)
	require.True(t, curSchema.Equals(addedSchema), "expected schema to match added schema")
	update := NewAddSchemaUpdate(addedSchema)
	require.Equal(t, builderRef.updates[0].Action(), update.Action())
	require.True(t, builderRef.updates[0].(*addSchemaUpdate).Schema.Equals(addedSchema), "expected schema to match added schema")
	require.Equal(t, builderRef.updates[1].(*setCurrentSchemaUpdate).SchemaID, -1)
}

func TestNoMetadataLogForCreateTable(t *testing.T) {
	builder := builderWithoutChanges(2)
	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)
	for thing := range meta.PreviousFiles() {
		require.FailNow(t, "expected no metadata log files, but got: %s", thing)
	}
}

func TestFromMetadataCreatesLogEntry(t *testing.T) {
	builder := builderWithoutChanges(2)
	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)

	// Create a new builder from the metadata
	loc := "s3://bla"
	newBuilder, err := MetadataBuilderFromBase(meta, &loc)
	require.NoError(t, err)
	require.NotNil(t, newBuilder)

	_, err = newBuilder.AddSortOrder(&UnsortedSortOrder)
	require.NoError(t, err)

	// Build the metadata again
	newMeta, err := newBuilder.Build()
	require.NoError(t, err)
	require.NotNil(t, newMeta)

	// Check that the metadata log has one entry
	require.Len(t, newMeta.(*metadataV2).MetadataLog, 1)
	require.Equal(t, newMeta.(*metadataV2).MetadataLog[0].MetadataFile, loc)
}

func TestSetRef(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       1,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.lastUpdatedMS + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}
	_, err := builder.AddSnapshot(&snapshot)
	require.NoError(t, err)
	_, err = builder.SetSnapshotRef(MainBranch, 10, BranchRef, WithMinSnapshotsToKeep(10))
	require.ErrorContains(t, err, "can't set snapshot ref main to unknown snapshot 10: snapshot with id 10 not found")
	_, err = builder.SetSnapshotRef(MainBranch, 1, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)
	require.Len(t, builder.snapshotList, 1)
	snap, err := builder.SnapshotByID(1)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, snap.SnapshotID, int64(1))
	require.True(t, snap.Equals(snapshot), "expected snapshot to match added snapshot")
	require.Len(t, builder.snapshotLog, 1)
}

func TestSnapshotLogSkipsIntermediate(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot1 := Snapshot{
		SnapshotID:       1,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.lastUpdatedMS + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}

	snapshot2 := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.lastUpdatedMS + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}
	_, err := builder.AddSnapshot(&snapshot1)
	require.NoError(t, err)
	_, err = builder.SetSnapshotRef(MainBranch, 1, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)

	_, err = builder.AddSnapshot(&snapshot2)
	require.NoError(t, err)
	_, err = builder.SetSnapshotRef(MainBranch, 2, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)

	require.NoError(t, err)

	res, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Len(t, res.(*metadataV2).SnapshotLog, 1)
	snap := res.(*metadataV2).SnapshotLog[0]
	require.NotNil(t, snap)
	require.Equal(t, snap, SnapshotLogEntry{
		SnapshotID:  2,
		TimestampMs: snapshot2.TimestampMs,
	}, "expected snapshot to match added snapshot")
	require.True(t, res.CurrentSnapshot().Equals(snapshot2))
}

func TestSetBranchSnapshotCreatesBranchIfNotExists(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.lastUpdatedMS + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}

	_, err := builder.AddSnapshot(&snapshot)
	require.NoError(t, err)
	_, err = builder.SetSnapshotRef("new_branch", 2, BranchRef)
	require.NoError(t, err)

	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)
	// Check that the branch was created
	ref := meta.(*metadataV2).SnapshotRefs["new_branch"]
	require.Len(t, meta.(*metadataV2).SnapshotRefs, 1)
	require.Equal(t, int64(2), ref.SnapshotID)
	require.Equal(t, BranchRef, ref.SnapshotRefType)
	require.True(t, builder.updates[0].(*addSnapshotUpdate).Snapshot.Equals(snapshot))
	require.Equal(t, "new_branch", builder.updates[1].(*setSnapshotRefUpdate).RefName)
	require.Equal(t, BranchRef, builder.updates[1].(*setSnapshotRefUpdate).RefType)
	require.Equal(t, int64(2), builder.updates[1].(*setSnapshotRefUpdate).SnapshotID)
}

func TestCannotAddDuplicateSnapshotID(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.lastUpdatedMS + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}
	_, err := builder.AddSnapshot(&snapshot)
	require.NoError(t, err)
	_, err = builder.AddSnapshot(&snapshot)
	require.ErrorContains(t, err, "can't add snapshot with id 2, already exists")
}

func TestAddIncompatibleCurrentSchemaFails(t *testing.T) {
	builder := builderWithoutChanges(2)
	addedSchema := iceberg.NewSchema(1)
	_, err := builder.AddSchema(addedSchema)
	require.NoError(t, err)
	_, err = builder.SetCurrentSchemaID(1)
	require.NoError(t, err)
	_, err = builder.Build()
	require.ErrorContains(t, err, "with source id 3 not found in schema")
}

func TestAddPartitionSpecForV1RequiresSequentialIDs(t *testing.T) {
	builder := builderWithoutChanges(1)

	// Add a partition spec with non-sequential IDs
	id := 1000
	id2 := 1002
	addedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(10),
		iceberg.AddPartitionFieldBySourceID(2, "y", iceberg.IdentityTransform{}, builder.CurrentSchema(), &id),
		iceberg.AddPartitionFieldBySourceID(3, "z", iceberg.IdentityTransform{}, builder.CurrentSchema(), &id2))
	require.NoError(t, err)

	_, err = builder.AddPartitionSpec(&addedSpec, false)
	require.ErrorContains(t, err, "v1 constraint: partition field IDs are not sequential: expected 1001, got 1002")
}

func TestExpireMetadataLog(t *testing.T) {
	builder1 := builderWithoutChanges(2)
	meta, err := builder1.Build()
	require.NoError(t, err)
	loc := "s3://bla"
	builder, err := MetadataBuilderFromBase(meta, &loc)
	require.NoError(t, err)
	_, err = builder.SetProperties(map[string]string{
		MetadataPreviousVersionsMaxKey: "2",
	})
	require.NoError(t, err)
	meta, err = builder.Build()
	require.NoError(t, err)
	require.Len(t, meta.(*metadataV2).MetadataLog, 1)

	location := "p"
	newBuilder, err := MetadataBuilderFromBase(meta, &location)
	require.NoError(t, err)
	_, err = newBuilder.SetProperties(map[string]string{
		"change_nr": "1",
	})
	require.NoError(t, err)
	meta, err = newBuilder.Build()
	require.NoError(t, err)
	require.Len(t, meta.(*metadataV2).MetadataLog, 2)

	newBuilder, err = MetadataBuilderFromBase(meta, &location)
	require.NoError(t, err)
	_, err = newBuilder.SetProperties(map[string]string{
		"change_nr": "2",
	})
	require.NoError(t, err)
	meta, err = newBuilder.Build()
	require.NoError(t, err)
	require.Len(t, meta.(*metadataV2).MetadataLog, 2)
}

func TestV2SequenceNumberCannotDecrease(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot1 := Snapshot{
		SnapshotID:       1,
		ParentSnapshotID: nil,
		SequenceNumber:   1,
		TimestampMs:      builder.lastUpdatedMS + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation:  OpAppend,
			Properties: map[string]string{},
		},
		SchemaID: &schemaID,
	}

	builderRef, err := builder.AddSnapshot(&snapshot1)
	require.NoError(t, err)

	builderRef, err = builderRef.SetSnapshotRef(MainBranch, 1, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)

	parentSnapshotID := int64(1)
	snapshot2 := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: &parentSnapshotID,
		SequenceNumber:   0, // Lower sequence number than previous
		TimestampMs:      builderRef.lastUpdatedMS + 1,
		ManifestList:     "/snap-0.avro",
		Summary: &Summary{
			Operation:  OpAppend,
			Properties: map[string]string{},
		},
		SchemaID: &schemaID,
	}
	_, err = builderRef.AddSnapshot(&snapshot2)
	require.ErrorContains(t, err, "can't add snapshot with sequence number 0, must be > than last sequence number 1")
}

func TestDefaultSpecCannotBeRemoved(t *testing.T) {
	builder := builderWithoutChanges(2)

	_, err := builder.RemovePartitionSpecs([]int{0})
	require.ErrorContains(t, err, "can't remove default partition spec with id 0")
}

func TestStatistics(t *testing.T) {
	// FIXME: iceberg-go cannot do statistics yet, so this test is not implemented
}

func TestAddPartitionStatistics(t *testing.T) {
	// FIXME: iceberg-go cannot do statistics yet, so this test is not implemented
}

func TestLastUpdateIncreasedForPropertyOnlyUpdate(t *testing.T) {
	builder := builderWithoutChanges(2)
	meta, err := builder.Build()
	require.NoError(t, err)
	lastUpdatedMS := builder.lastUpdatedMS
	time.Sleep(5 * time.Millisecond)
	// Set a property

	location := "some-location"

	newBuilder, err := MetadataBuilderFromBase(meta, &location)
	require.NoError(t, err)

	_, err = newBuilder.SetProperties(map[string]string{
		"foo": "bar",
	})
	require.NoError(t, err)
	newMeta, err := newBuilder.Build()
	require.NoError(t, err)
	require.NotNil(t, newMeta)

	// Check that the last updated timestamp has increased
	require.Greater(t, newMeta.LastUpdatedMillis(), lastUpdatedMS, "expected last updated timestamp to increase after property update")
}

func TestConstructDefaultMainBranch(t *testing.T) {
	// TODO: Not sure what this test is supposed to do Rust: `test_construct_default_main_branch`
	meta, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.NotNil(t, meta)

	builder, err := MetadataBuilderFromBase(meta, nil)
	require.NoError(t, err)

	meta, err = builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.Equal(t, meta.(*metadataV2).SnapshotRefs[MainBranch].SnapshotID, meta.CurrentSnapshot().SnapshotID)
}

func TestActiveSchemaCannotBeRemoved(t *testing.T) {
	builder := builderWithoutChanges(2)

	// Try to remove the current schema
	_, err := builder.RemoveSchemas([]int{0})
	require.ErrorContains(t, err, "can't remove current schema with id 0")
}

func TestRemoveSchemas(t *testing.T) {
	meta, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.Len(t, meta.Schemas(), 2, "expected 2 schemas in the metadata")
	builder, err := MetadataBuilderFromBase(meta, nil)
	require.NoError(t, err)
	_, err = builder.RemoveSchemas([]int{0})
	require.NoError(t, err, "expected to remove schema with ID 1")
	newMeta, err := builder.Build()
	require.NoError(t, err)
	require.Len(t, newMeta.Schemas(), 1, "expected 1 schema in the metadata after removal")
	require.Equal(t, 1, newMeta.CurrentSchema().ID, "expected current schema to be 1")
	require.Equal(t, 1, newMeta.(*metadataV2).CurrentSchemaID)
	require.Len(t, builder.updates, 1, "expected one update for schema removal")
	require.Equal(t, builder.updates[0].Action(), UpdateRemoveSchemas)
	require.Equal(t, builder.updates[0].(*removeSchemasUpdate).SchemaIDs, []int{0}, "expected schema ID 0 to be removed")
}
