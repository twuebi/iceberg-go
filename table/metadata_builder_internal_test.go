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
	"github.com/apache/iceberg-go"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"testing"
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

const TestLocation = "s3://bucket/test/location"
const LastAssignedColumnID = 3

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

	require.Equal(t, meta.FormatVersion(), 1)
	require.Equal(t, meta.Location, TestLocation)
	require.Equal(t, meta.CurrentSchema().ID, 0)
	require.Equal(t, meta.DefaultPartitionSpec(), 0)
	require.Equal(t, meta.DefaultSortOrder(), 1)
	require.Equal(t, meta.LastPartitionSpecID(), 1000)
	require.Equal(t, meta.LastColumnID(), 3)
	require.Equal(t, len(meta.Snapshots()), 0)
	require.Nil(t, meta.CurrentSnapshot())
	for range meta.Refs() {
		t.Fatalf("refs should be empty.")
	}
	require.Equal(t, len(meta.Properties()), 0)
	for range meta.PreviousFiles() {
		t.Fatalf("metadata log should be empty.")
	}
	require.Equal(t, meta.LastSequenceNumber(), 0)
	require.Equal(t, meta.LastColumnID(), LastAssignedColumnID)
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

	require.Equal(t, meta.FormatVersion(), 2)
	require.Equal(t, meta.Location, TestLocation)
	require.Equal(t, meta.CurrentSchema().ID, 0)
	require.Equal(t, meta.DefaultPartitionSpec(), 0)
	require.Equal(t, meta.DefaultSortOrder(), 0)
	require.Equal(t, meta.LastPartitionSpecID(), 999)
	require.Equal(t, meta.LastColumnID(), 0)
	require.Equal(t, len(meta.Snapshots()), 0)
	require.Nil(t, meta.CurrentSnapshot())
	for range meta.Refs() {
		t.Fatalf("refs should be empty.")
	}
	require.Equal(t, len(meta.Properties()), 0)
	for range meta.PreviousFiles() {
		t.Fatalf("metadata log should be empty.")
	}
	require.Equal(t, meta.LastSequenceNumber(), 0)
	require.Equal(t, meta.LastColumnID(), 0)
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
	spec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(20),
		iceberg.AddPartitionFieldByName("a", "a", iceberg.IdentityTransform{}, schema, nil),
		iceberg.AddPartitionFieldByName("struct.nested", "nested_partition", iceberg.IdentityTransform{}, schema, nil),
	)

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

	expectedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(0),
		iceberg.AddPartitionFieldByName("a", "a", iceberg.IdentityTransform{}, expectedSchema, nil),
		iceberg.AddPartitionFieldByName("struct.nested", "nested_partition", iceberg.IdentityTransform{}, expectedSchema, nil),
	)

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

func TestAddPartitionSpec(t *testing.T) {
	builder := builderWithoutChanges(2)
	builderRef := &builder
	i := 1000
	addedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(10),
		iceberg.AddPartitionFieldBySourceID(2, "y", iceberg.IdentityTransform{}, builder.schemaList[0], &i),
		iceberg.AddPartitionFieldBySourceID(3, "z", iceberg.IdentityTransform{}, builder.schemaList[0], nil),
	)
	require.NoError(t, err)

	builderRef, err = builderRef.AddPartitionSpec(&addedSpec, false)
	require.NoError(t, err)
	metadata, err := builderRef.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)

	i2 := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(1),
		iceberg.AddPartitionFieldBySourceID(2, "y", iceberg.IdentityTransform{}, builder.schemaList[0], &i),
		iceberg.AddPartitionFieldBySourceID(3, "z", iceberg.IdentityTransform{}, builder.schemaList[0], &i2),
	)
	require.Equal(t, metadata.DefaultPartitionSpec(), 0)
	require.Equal(t, metadata.LastPartitionSpecID(), &i2)
	found := false
	for _, part := range metadata.PartitionSpecs() {
		if part.ID() == 1 {
			found = true
			require.True(t, part.Equals(expectedSpec), "expected partition spec to match added spec")
		}
	}
	require.True(t, found, "expected partition spec to be added")

	// newBuilder := MetadataBuilderFromBase(metadata)
	// TODO: add remove_partition_spec method to MetadataBuilder
	// Remove the spec
	//let build_result = build_result
	//.metadata
	//.into_builder(Some(
	//	"s3://bucket/test/location/metadata/metadata1.json".to_string(),
	//))
	//.remove_partition_specs(&[1])
	//.unwrap()
	//.build()
	//.unwrap();
	//
	//assert_eq!(build_result.changes.len(), 1);
	//assert_eq!(build_result.metadata.partition_specs.len(), 1);
	//assert!(build_result.metadata.partition_spec_by_id(1).is_none());
}

func TestSetDefaultPartitionSpec(t *testing.T) {
	builder := builderWithoutChanges(2)
	curSchema, err := builder.GetSchemaByID(builder.currentSchemaID)
	require.NoError(t, err)
	// Add a partition spec
	addedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(10),
		iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, nil))
	require.NoError(t, err)
	builderRef, err := builder.AddPartitionSpec(&addedSpec, false)
	require.NoError(t, err)
	// Set the default partition spec
	builderRef, err = builderRef.SetDefaultSpecID(-1)
	require.NoError(t, err)

	id := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(1),
		iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, &id))
	require.True(t, builderRef.HasChanges())
	require.Equal(t, len(builderRef.updates), 2)
	require.True(t, builderRef.updates[0].(*addPartitionSpecUpdate).Spec.Equals(addedSpec))
	require.Equal(t, -1, builderRef.updates[1].(*setDefaultSpecUpdate).SpecID)
	metadata, err := builderRef.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)

	require.Equal(t, metadata.DefaultPartitionSpec(), 1)
	require.True(t, expectedSpec.Equals(metadata.PartitionSpec()), "expected partition spec to match added spec")
	require.Equal(t, *metadata.LastPartitionSpecID(), 1001)
}

func TestSetExistingDefaultPartitionSpec(t *testing.T) {
	// Arrange: Create a builder, get the current schema, and add a partition spec.
	builder := builderWithoutChanges(2)
	curSchema, err := builder.GetSchemaByID(builder.currentSchemaID)
	require.NoError(t, err)

	addedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(10),
		iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, nil))
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
	expectedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(1),
		iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, &id))
	require.NoError(t, err)
	require.True(t, expectedSpec.Equals(metadata.PartitionSpec()), "expected partition spec to match added spec")

	// ---

	// Arrange: Create a new builder from the previously built metadata.
	newBuilder, err := MetadataBuilderFromBase(metadata)
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
