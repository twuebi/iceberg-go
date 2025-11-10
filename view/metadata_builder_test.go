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

package view_test

import (
	"iter"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/view"
	"github.com/stretchr/testify/require"
)

func TestNewViewMetadataBuilder(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)
	require.NotNil(t, builder)
}

func TestNewViewMetadataBuilderUnsupportedVersion(t *testing.T) {
	_, err := view.NewMetadataBuilder(0)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)

	_, err = view.NewMetadataBuilder(2)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)
}

func TestViewMetadataBuilderFrom(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	baseMetadata := &testMetadata{
		uuid:           "test-uuid",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		currentVersion: &version,
		versions:       []view.Version{version},
		versionLog:     []view.VersionLogEntry{{TimestampMs: 1234567890, VersionID: 1}},
		properties:     map[string]string{"key": "value"},
	}

	builder := view.MetadataBuilderFrom(baseMetadata, "s3://bucket/view/metadata/v1.json")
	require.NotNil(t, builder)
}

type testMetadata struct {
	uuid           string
	formatVersion  int
	location       string
	schemas        []*iceberg.Schema
	currentVersion *view.Version
	versions       []view.Version
	versionLog     []view.VersionLogEntry
	properties     map[string]string
}

func (m *testMetadata) ViewUUID() string               { return m.uuid }
func (m *testMetadata) FormatVersion() int             { return m.formatVersion }
func (m *testMetadata) Location() string               { return m.location }
func (m *testMetadata) CurrentVersion() *view.Version  { return m.currentVersion }
func (m *testMetadata) Properties() iceberg.Properties { return m.properties }

func (m *testMetadata) Schemas() iter.Seq[*iceberg.Schema] {
	return func(yield func(*iceberg.Schema) bool) {
		for _, s := range m.schemas {
			if !yield(s) {
				return
			}
		}
	}
}

func (m *testMetadata) Versions() iter.Seq[view.Version] {
	return func(yield func(view.Version) bool) {
		for _, v := range m.versions {
			if !yield(v) {
				return
			}
		}
	}
}

func (m *testMetadata) VersionLog() iter.Seq[view.VersionLogEntry] {
	return func(yield func(view.VersionLogEntry) bool) {
		for _, e := range m.versionLog {
			if !yield(e) {
				return
			}
		}
	}
}

func TestAssignUUID(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid-123")
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid-123")
	require.NoError(t, err)
}

func TestAssignUUIDEmpty(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot set uuid to empty string")
}

func TestAssignUUIDReassignment(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("uuid-1")
	require.NoError(t, err)

	err = builder.AssignUUID("uuid-2")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot reassign uuid")
}

func TestSetLocation(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)
}

func TestSetLocationEmpty(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.SetLocation("")
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestUpgradeFormatVersion(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.UpgradeFormatVersion(1)
	require.NoError(t, err)
}

func TestDowngradeFormatVersionFails(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.UpgradeFormatVersion(0)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)
}

func TestUpgradeFormatVersionUnsupported(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.UpgradeFormatVersion(2)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)
}

func TestSetProperties(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{
		"key1": "updated",
	})
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{})
	require.NoError(t, err)
}

func TestRemoveProperties(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})
	require.NoError(t, err)

	err = builder.RemoveProperties([]string{"key1"})
	require.NoError(t, err)

	err = builder.RemoveProperties([]string{})
	require.NoError(t, err)
}

func TestAddSchema(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	retrieved, err := builder.GetSchemaByID(0)
	require.NoError(t, err)
	require.Equal(t, 0, retrieved.ID)
}

func TestAddSchemaNil(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AddSchema(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot add nil schema")
}

func TestGetSchemaByID(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	retrieved, err := builder.GetSchemaByID(0)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, 0, retrieved.ID)
}

func TestGetSchemaByIDNotFound(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	_, err = builder.GetSchemaByID(999)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema with id 999 not found")
}

func TestSchemaDeduplication(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema1 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	schema2 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema1)
	require.NoError(t, err)
	require.Equal(t, 0, schema1.ID)

	err = builder.AddSchema(schema2)
	require.NoError(t, err)
	require.Equal(t, 0, schema2.ID)
}

func TestSchemaIDReassignment(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema1 := iceberg.NewSchema(5,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema1)
	require.NoError(t, err)

	schema2 := iceberg.NewSchema(10,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema2)
	require.NoError(t, err)

	require.Equal(t, schema1.ID, schema2.ID)
}

func TestMultipleSchemas(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema1 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	schema2 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	err = builder.AddSchema(schema1)
	require.NoError(t, err)

	err = builder.AddSchema(schema2)
	require.NoError(t, err)

	require.NotEqual(t, schema1.ID, schema2.ID)
	require.Equal(t, 0, schema1.ID)
	require.Equal(t, 1, schema2.ID)
}

func TestAddVersion(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version)
	require.NoError(t, err)

	retrieved, err := builder.GetVersionByID(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), retrieved.VersionID)
}

func TestAddVersionNil(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AddVersion(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot add nil version")
}

func TestVersionWithNonExistentSchema(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         999,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot add version with unknown schema")
}

func TestMultipleSQLForSameDialect(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:   1,
		SchemaID:    0,
		TimestampMs: 1234567890,
		Summary:     map[string]string{"operation": "create"},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT id FROM t", Dialect: "spark"},
		},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot add multiple queries for dialect spark")
}

func TestMultipleSQLForSameDialectCaseInsensitive(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:   1,
		SchemaID:    0,
		TimestampMs: 1234567890,
		Summary:     map[string]string{"operation": "create"},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT id FROM t", Dialect: "SPARK"},
		},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot add multiple queries for dialect")
}

func TestGetVersionByID(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version)
	require.NoError(t, err)

	retrieved, err := builder.GetVersionByID(1)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, int64(1), retrieved.VersionID)
}

func TestGetVersionByIDNotFound(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	_, err = builder.GetVersionByID(999)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version with id 999 not found")
}

func TestVersionDeduplication(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version1 := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	version2 := &view.Version{
		VersionID:        2,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version1)
	require.NoError(t, err)
	require.Equal(t, int64(1), version1.VersionID)

	err = builder.AddVersion(version2)
	require.NoError(t, err)
	require.Equal(t, int64(1), version2.VersionID)
}

func TestVersionIDReassignment(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version1 := &view.Version{
		VersionID:        5,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version1)
	require.NoError(t, err)

	version2 := &view.Version{
		VersionID:        10,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version2)
	require.NoError(t, err)

	require.Equal(t, version1.VersionID, version2.VersionID)
}

func TestMultipleVersions(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version1 := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	version2 := &view.Version{
		VersionID:        2,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "replace"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT id FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version1)
	require.NoError(t, err)

	err = builder.AddVersion(version2)
	require.NoError(t, err)

	require.NotEqual(t, version1.VersionID, version2.VersionID)
	require.Equal(t, int64(1), version1.VersionID)
	require.Equal(t, int64(2), version2.VersionID)
}

func TestSetCurrentVersionID(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(1)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(1)
	require.NoError(t, err)
}

func TestSetCurrentVersionIDLastAdded(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(view.LastAdded)
	require.NoError(t, err)
}

func TestSetCurrentVersionIDInvalid(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(999)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot set current version")
}

func TestSetCurrentVersionIDLastAddedWithoutVersion(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(view.LastAdded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot set last added version")
}

func TestSetCurrentVersion(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.SetCurrentVersion(version, schema)
	require.NoError(t, err)
}

func TestBuildSuccess(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.SetCurrentVersion(version, schema)
	require.NoError(t, err)

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "test-uuid", result.Metadata.ViewUUID())
	require.Equal(t, "s3://bucket/view", result.Metadata.Location())
	require.Equal(t, int64(1), result.Metadata.CurrentVersion().VersionID)
}

func TestBuildMissingLocation(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid")
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.SetCurrentVersion(version, schema)
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.ErrorIs(t, err, view.ErrInvalidViewMetadata)
}

func TestBuildMissingVersions(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least one version is required")
}

func TestBuildMissingUUID(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.SetCurrentVersion(version, schema)
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.ErrorIs(t, err, view.ErrInvalidViewMetadata)
}

func TestBuildMissingCurrentVersion(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	err = builder.AddVersion(version)
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.Contains(t, err.Error(), "current-version-id is required")
}

func TestNewViewMetadata(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	builder, err := view.NewViewMetadata(
		"s3://bucket/view",
		schema,
		version,
		map[string]string{"key": "value"},
	)
	require.NoError(t, err)
	require.NotNil(t, builder)

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "s3://bucket/view", result.Metadata.Location())
	require.Equal(t, int64(1), result.Metadata.CurrentVersion().VersionID)
	require.Equal(t, "value", result.Metadata.Properties()["key"])
}

func TestNewViewMetadataWithoutProperties(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	builder, err := view.NewViewMetadata(
		"s3://bucket/view",
		schema,
		version,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, builder)

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestExpiration(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	v1 := view.Version{VersionID: 1, SchemaID: 0, TimestampMs: 1000000001, Summary: map[string]string{"operation": "create-1"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 1", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v2 := view.Version{VersionID: 2, SchemaID: 0, TimestampMs: 1000000002, Summary: map[string]string{"operation": "create-2"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v3 := view.Version{VersionID: 3, SchemaID: 0, TimestampMs: 1000000003, Summary: map[string]string{"operation": "create-3"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 3", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v4 := view.Version{VersionID: 4, SchemaID: 0, TimestampMs: 1000000004, Summary: map[string]string{"operation": "create-4"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 4", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v5 := view.Version{VersionID: 5, SchemaID: 0, TimestampMs: 1000000005, Summary: map[string]string{"operation": "create-5"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 5", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}

	baseMetadata := &testMetadata{
		uuid:           "test-uuid",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1, v2, v3, v4, v5},
		currentVersion: &v5,
		properties:     map[string]string{view.PropertyVersionHistorySize: "3"},
	}

	builder := view.MetadataBuilderFrom(baseMetadata, "")

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)

	versions := make([]view.Version, 0)
	for v := range result.Metadata.Versions() {
		versions = append(versions, v)
	}
	require.LessOrEqual(t, len(versions), 3)

	hasCurrentVersion := false
	for _, v := range versions {
		if v.VersionID == 5 {
			hasCurrentVersion = true
			break
		}
	}
	require.True(t, hasCurrentVersion, "current version should not be expired")
}

func TestCurrentVersionIsNeverExpired(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		version := &view.Version{
			VersionID:        int64(i),
			SchemaID:         0,
			TimestampMs:      int64(1000000000 + i*1000),
			Summary:          map[string]string{"operation": "create"},
			Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
			DefaultNamespace: []string{"default"},
		}
		err = builder.AddVersion(version)
		require.NoError(t, err)
	}

	err = builder.SetProperties(map[string]string{
		view.PropertyVersionHistorySize: "1",
	})
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(1)
	require.NoError(t, err)

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)

	hasCurrentVersion := false
	for v := range result.Metadata.Versions() {
		if v.VersionID == 1 {
			hasCurrentVersion = true
			break
		}
	}
	require.True(t, hasCurrentVersion, "current version must never be expired")
}

func TestUpdateHistory(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	v1 := view.Version{VersionID: 1, SchemaID: 0, TimestampMs: 1000000001, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 1", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v2 := view.Version{VersionID: 2, SchemaID: 0, TimestampMs: 1000000002, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v3 := view.Version{VersionID: 3, SchemaID: 0, TimestampMs: 1000000003, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 3", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v5 := view.Version{VersionID: 5, SchemaID: 0, TimestampMs: 1000000005, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 5", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}

	baseMetadata := &testMetadata{
		uuid:          "test-uuid",
		formatVersion: 1,
		location:      "s3://bucket/view",
		schemas:       []*iceberg.Schema{schema},
		versions:      []view.Version{v1, v2, v3, v5},
		versionLog: []view.VersionLogEntry{
			{VersionID: 1, TimestampMs: 1000000001},
			{VersionID: 2, TimestampMs: 1000000002},
			{VersionID: 3, TimestampMs: 1000000003},
			{VersionID: 4, TimestampMs: 1000000004},
			{VersionID: 5, TimestampMs: 1000000005},
		},
		currentVersion: &v5,
		properties:     map[string]string{view.PropertyVersionHistorySize: "10"},
	}

	builder := view.MetadataBuilderFrom(baseMetadata, "")

	result, err := builder.Build()
	require.NoError(t, err)

	historyEntries := make([]view.VersionLogEntry, 0)
	for entry := range result.Metadata.VersionLog() {
		historyEntries = append(historyEntries, entry)
	}

	for _, entry := range historyEntries {
		require.NotEqual(t, int64(1), entry.VersionID, "history before gap should be cleared")
		require.NotEqual(t, int64(2), entry.VersionID, "history before gap should be cleared")
		require.NotEqual(t, int64(3), entry.VersionID, "history before gap should be cleared")
	}
}

func TestNegativeHistorySize(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1000000001,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}
	err = builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(1)
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{
		view.PropertyVersionHistorySize: "-1",
	})
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be positive")
}

func TestZeroHistorySize(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	version := &view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1000000001,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}
	err = builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(1)
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{
		view.PropertyVersionHistorySize: "0",
	})
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be positive")
}

func TestMetadataLocationAndChanges(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	v1 := view.Version{VersionID: 1, SchemaID: 0, TimestampMs: 1000000001, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 1", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}

	baseMetadata := &testMetadata{
		uuid:           "test-uuid",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1},
		currentVersion: &v1,
		properties:     map[string]string{},
	}

	builder := view.MetadataBuilderFrom(baseMetadata, "s3://bucket/metadata.json")

	version := &view.Version{
		VersionID:        2,
		SchemaID:         0,
		TimestampMs:      1000000002,
		Summary:          map[string]string{"operation": "replace"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}
	err := builder.AddVersion(version)
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata location and changes")
}

func TestDroppingDialectFailsByDefault(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	v1 := view.Version{
		VersionID:   1,
		SchemaID:    0,
		TimestampMs: 1000000001,
		Summary:     map[string]string{},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT 1", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT 1", Dialect: "trino"},
		},
		DefaultNamespace: []string{"default"},
	}

	baseMetadata := &testMetadata{
		uuid:           "test-uuid",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1},
		currentVersion: &v1,
		properties:     map[string]string{},
	}

	builder := view.MetadataBuilderFrom(baseMetadata, "")

	version := &view.Version{
		VersionID:        2,
		SchemaID:         0,
		TimestampMs:      1000000002,
		Summary:          map[string]string{"operation": "replace"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}
	err := builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(2)
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.Contains(t, err.Error(), "dropping dialects")
	require.Contains(t, err.Error(), "trino")
}

func TestDroppingDialectDoesNotFailWhenAllowed(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	v1 := view.Version{
		VersionID:   1,
		SchemaID:    0,
		TimestampMs: 1000000001,
		Summary:     map[string]string{},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT 1", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT 1", Dialect: "trino"},
		},
		DefaultNamespace: []string{"default"},
	}

	baseMetadata := &testMetadata{
		uuid:           "test-uuid",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1},
		currentVersion: &v1,
		properties:     map[string]string{view.PropertyReplaceDropDialectAllowed: "true"},
	}

	builder := view.MetadataBuilderFrom(baseMetadata, "")

	version := &view.Version{
		VersionID:        2,
		SchemaID:         0,
		TimestampMs:      1000000002,
		Summary:          map[string]string{"operation": "replace"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}
	err := builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(2)
	require.NoError(t, err)

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestAddingDialectSucceeds(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	v1 := view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1000000001,
		Summary:          map[string]string{},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 1", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	baseMetadata := &testMetadata{
		uuid:           "test-uuid",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1},
		currentVersion: &v1,
		properties:     map[string]string{},
	}

	builder := view.MetadataBuilderFrom(baseMetadata, "")

	version := &view.Version{
		VersionID:   2,
		SchemaID:    0,
		TimestampMs: 1000000002,
		Summary:     map[string]string{"operation": "replace"},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT 2", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT 2", Dialect: "trino"},
		},
		DefaultNamespace: []string{"default"},
	}
	err := builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(2)
	require.NoError(t, err)

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestVersionCopyDeepCopy(t *testing.T) {
	catalog := "test-catalog"
	original := &view.Version{
		VersionID:        1,
		SchemaID:         5,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create", "key": "value"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultCatalog:   &catalog,
		DefaultNamespace: []string{"db", "schema"},
	}

	copied := original.Copy()

	require.NotNil(t, copied)
	require.NotSame(t, original, copied)
	require.Equal(t, original.VersionID, copied.VersionID)
	require.Equal(t, original.SchemaID, copied.SchemaID)
	require.Equal(t, original.TimestampMs, copied.TimestampMs)

	require.Equal(t, original.Summary, copied.Summary)
	require.Equal(t, original.Representations, copied.Representations)
	require.Equal(t, *original.DefaultCatalog, *copied.DefaultCatalog)
	require.NotSame(t, original.DefaultCatalog, copied.DefaultCatalog)
	require.Equal(t, original.DefaultNamespace, copied.DefaultNamespace)

	original.Summary["modified"] = "changed"
	require.NotContains(t, copied.Summary, "modified")

	original.DefaultNamespace[0] = "changed"
	require.Equal(t, "db", copied.DefaultNamespace[0])
}

func TestVersionCopyNil(t *testing.T) {
	var version *view.Version
	copied := version.Copy()
	require.Nil(t, copied)
}

func TestSetCurrentVersionUpdatesSchemaID(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	err = builder.AssignUUID("test-uuid")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	schema1 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	schema2 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	version := &view.Version{
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	err = builder.SetCurrentVersion(version, schema1)
	require.NoError(t, err)
	require.Equal(t, 0, version.SchemaID)

	err = builder.SetCurrentVersion(version, schema2)
	require.NoError(t, err)
	require.Equal(t, 0, version.SchemaID)

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)

	currentVersion := result.Metadata.CurrentVersion()
	require.NotNil(t, currentVersion)
	require.Equal(t, 1, currentVersion.SchemaID)
}
