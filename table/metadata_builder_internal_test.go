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
		iceberg.AddPartitionField("a", "a", schema, iceberg.IdentityTransform{}),
		iceberg.AddPartitionField("struct.nested", "nested_partition", schema, iceberg.IdentityTransform{}),
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
		iceberg.AddPartitionField("a", "a", expectedSchema, iceberg.IdentityTransform{}),
		iceberg.AddPartitionField("struct.nested", "nested_partition", expectedSchema, iceberg.IdentityTransform{}),
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
