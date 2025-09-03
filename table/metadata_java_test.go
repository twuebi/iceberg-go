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

package table_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Tests ported from Java: TestTableMetadata.java

// Java: TestTableMetadata.testV2UUIDValidation
func TestV2UUIDValidation(t *testing.T) {
	// V2 format requires a UUID, attempting to create metadata without one should fail
	schema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	spec := iceberg.NewPartitionSpec()
	sortOrder := table.UnsortedSortOrder

	// Try to create V2 metadata without UUID
	// Note: In Go implementation, we need to verify this during Build() or in constructor
	builder, err := table.NewMetadataBuilderFromPieces(
		schema,
		spec,
		sortOrder,
		"s3://bucket/test/location",
		2, // V2 format
		map[string]string{},
	)
	require.NoError(t, err) // Builder creation succeeds

	// Force empty UUID for V2 format to test validation
	builder.SetUUID(uuid.Nil)
	_, err = builder.Build()

	require.Error(t, err)
	require.Contains(t, err.Error(), "UUID is required in format v2")
}

// Java: TestTableMetadata.testUpgradeMetadataThroughTableProperty
func TestUpgradeMetadataThroughTableProperty(t *testing.T) {
	testCases := []struct {
		name              string
		baseFormatVersion int
		newFormatVersion  int
	}{
		{"V1 to V2", 1, 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema := iceberg.NewSchema(
				0,
				iceberg.NestedField{ID: 10, Name: "x", Type: iceberg.PrimitiveTypes.String, Required: true},
			)

			// Create metadata with base format version
			meta, err := table.NewMetadata(
				schema,
				iceberg.UnpartitionedSpec,
				table.UnsortedSortOrder,
				"s3://bucket/test/location",
				map[string]string{
					"format-version": string(rune(tc.baseFormatVersion + '0')),
					"key":            "val",
				},
			)
			require.NoError(t, err)
			require.Equal(t, tc.baseFormatVersion, meta.Version())

			// Upgrade format version through property
			builder, err := table.MetadataBuilderFromBase(meta, nil)
			require.NoError(t, err)

			_, err = builder.SetProperties(map[string]string{
				"format-version": string(rune(tc.newFormatVersion + '0')),
				"key2":           "val2",
			})
			require.NoError(t, err)

			upgradedMeta, err := builder.Build()
			require.NoError(t, err)

			// Format version should be upgraded
			require.Equal(t, tc.newFormatVersion, upgradedMeta.Version())

			// Properties should not contain format-version (it's reserved)
			props := upgradedMeta.Properties()
			_, hasFormatVersion := props["format-version"]
			require.False(t, hasFormatVersion, "format-version should not be in properties")

			// New property should be present
			require.Equal(t, "val2", props["key2"])
		})
	}
}

// Java: TestTableMetadata.testAddPreviousMetadataRemoveOne
func TestAddPreviousMetadataRemoveOne(t *testing.T) {
	schema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	// Create initial metadata
	meta, err := table.NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder,
		"s3://bucket/test/location",
		map[string]string{},
	)
	require.NoError(t, err)

	// Create builder with metadata log
	loc1 := "s3://bucket/v1.metadata.json"
	builder1, err := table.MetadataBuilderFromBase(meta, &loc1)
	require.NoError(t, err)

	// Set max metadata log entries to 2
	_, err = builder1.SetProperties(map[string]string{
		table.MetadataPreviousVersionsMaxKey: "2",
	})
	require.NoError(t, err)

	meta1, err := builder1.Build()
	require.NoError(t, err)

	// Add second metadata log entry
	loc2 := "s3://bucket/v2.metadata.json"
	builder2, err := table.MetadataBuilderFromBase(meta1, &loc2)
	require.NoError(t, err)

	_, err = builder2.SetProperties(map[string]string{
		"test": "value",
	})
	require.NoError(t, err)

	meta2, err := builder2.Build()
	require.NoError(t, err)

	// Should have 2 metadata log entries (max limit)
	var logEntries []table.MetadataLogEntry
	for entry := range meta2.PreviousFiles() {
		logEntries = append(logEntries, entry)
	}
	require.Len(t, logEntries, 2)

	// Add third metadata log entry - should remove oldest
	loc3 := "s3://bucket/v3.metadata.json"
	builder3, err := table.MetadataBuilderFromBase(meta2, &loc3)
	require.NoError(t, err)

	_, err = builder3.SetProperties(map[string]string{
		"test2": "value2",
	})
	require.NoError(t, err)

	meta3, err := builder3.Build()
	require.NoError(t, err)

	// Should still have 2 metadata log entries (oldest removed)
	var finalLogEntries []table.MetadataLogEntry
	for entry := range meta3.PreviousFiles() {
		finalLogEntries = append(finalLogEntries, entry)
	}
	require.Len(t, finalLogEntries, 2)

	// Verify the oldest entry was removed
	hasOldest := false
	for _, entry := range finalLogEntries {
		if entry.MetadataFile == loc1 {
			hasOldest = true
			break
		}
	}
	require.False(t, hasOldest, "Oldest metadata log entry should have been removed")
}

// Java: TestTableMetadata.testParseSchemaIdentifierFields
func TestParseSchemaIdentifierFields(t *testing.T) {
	// Test that identifier fields are properly tracked in schemas
	schema1 := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	schema2 := iceberg.NewSchemaWithIdentifiers(
		1,
		[]int{1, 2}, // identifier field IDs
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	builder, err := table.NewMetadataBuilder()
	require.NoError(t, err)

	_, err = builder.AddSchema(schema1)
	require.NoError(t, err)

	_, err = builder.AddSchema(schema2)
	require.NoError(t, err)

	_, err = builder.SetCurrentSchemaID(1)
	require.NoError(t, err)

	meta, err := builder.Build()
	require.NoError(t, err)

	// Verify identifier fields
	schemas := meta.Schemas()
	require.Len(t, schemas, 2)

	require.Empty(t, schemas[0].IdentifierFieldIDs)
	require.Equal(t, []int{1, 2}, schemas[1].IdentifierFieldIDs)
}

// Java: TestTableMetadata.testUpdateSchema
func TestUpdateSchema(t *testing.T) {
	// Test schema updates and evolution
	schema1 := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
	)

	meta, err := table.NewMetadata(
		schema1,
		iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder,
		"s3://bucket/test/location",
		map[string]string{},
	)
	require.NoError(t, err)

	require.Equal(t, 0, meta.CurrentSchema().ID)
	require.Len(t, meta.Schemas(), 1)
	require.Equal(t, 1, meta.LastColumnID())

	// Update schema by adding a field
	schema2 := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
		iceberg.NestedField{ID: 2, Name: "x", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	builder, err := table.MetadataBuilderFromBase(meta, nil)
	require.NoError(t, err)

	_, err = builder.AddSchema(schema2)
	require.NoError(t, err)

	_, err = builder.SetCurrentSchemaID(-1) // Use last added
	require.NoError(t, err)

	updatedMeta, err := builder.Build()
	require.NoError(t, err)

	require.Equal(t, 1, updatedMeta.CurrentSchema().ID)
	require.Len(t, updatedMeta.Schemas(), 2)
	require.Equal(t, 2, updatedMeta.LastColumnID())

	// Verify both schemas are preserved
	schemas := updatedMeta.Schemas()
	require.True(t, schemas[0].Equals(schema1))
	require.True(t, schemas[1].Equals(schema2))
}

// Java: TestTableMetadata test for backward compatibility
func TestBackwardCompatV1ToV2(t *testing.T) {
	// Test that V1 metadata can be upgraded to V2
	schema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	// Create V1 metadata
	v1Meta, err := table.NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder,
		"s3://bucket/test/location",
		map[string]string{"format-version": "1"},
	)
	require.NoError(t, err)
	require.Equal(t, 1, v1Meta.Version())

	// Upgrade to V2
	builder, err := table.MetadataBuilderFromBase(v1Meta, nil)
	require.NoError(t, err)

	_, err = builder.SetFormatVersion(2)
	require.NoError(t, err)

	v2Meta, err := builder.Build()
	require.NoError(t, err)

	// Verify upgrade
	require.Equal(t, 2, v2Meta.Version())
	require.NotEqual(t, uuid.Nil, v2Meta.TableUUID())
	require.Equal(t, int64(0), v2Meta.LastSequenceNumber())

	// Schema should be preserved
	require.True(t, v2Meta.CurrentSchema().Equals(schema))
}