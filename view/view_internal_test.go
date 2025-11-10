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

package view

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidMetadataDeserialization(t *testing.T) {
	validJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"properties": {
			"comment": "Daily event counts"
		},
		"versions": [{
			"version-id": 1,
			"timestamp-ms": 1573518431292,
			"schema-id": 1,
			"default-catalog": "prod",
			"default-namespace": ["default"],
			"summary": {
				"operation": "create",
				"engine-name": "Spark",
				"engine-version": "3.3.2"
			},
			"representations": [{
				"type": "sql",
				"sql": "SELECT COUNT(*) FROM events",
				"dialect": "spark"
			}]
		}],
		"schemas": [{
			"schema-id": 1,
			"type": "struct",
			"fields": [{
				"id": 1,
				"name": "event_count",
				"required": false,
				"type": "long"
			}]
		}],
		"version-log": [{
			"timestamp-ms": 1573518431292,
			"version-id": 1
		}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(validJSON), &meta)
	require.NoError(t, err)

	assert.Equal(t, "fa6506c3-7681-40c8-86dc-e36561f83385", meta.ViewUUID())
	assert.Equal(t, 1, meta.FormatVersion())
	assert.Equal(t, "s3://bucket/warehouse/default.db/event_agg", meta.Location())
	assert.Equal(t, int64(1), meta.CurrentVersionId)
}

func TestMissingViewUUID(t *testing.T) {
	invalidJSON := `{
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "view-uuid is required")
}

func TestMissingLocation(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "location is required")
}

func TestMissingFormatVersion(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadataFormatVersion))
	assert.Contains(t, err.Error(), "format-version is required")
}

func TestInvalidFormatVersion(t *testing.T) {
	testCases := []struct {
		name string
		json string
	}{
		{
			"version 0",
			`{
				"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
				"format-version": 0,
				"location": "s3://bucket/warehouse/default.db/event_agg",
				"current-version-id": 1,
				"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
				"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
				"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
			}`,
		},
		{
			"version 2",
			`{
				"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
				"format-version": 2,
				"location": "s3://bucket/warehouse/default.db/event_agg",
				"current-version-id": 1,
				"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
				"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
				"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
			}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var meta metadata
			err := json.Unmarshal([]byte(tc.json), &meta)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidViewMetadataFormatVersion))
		})
	}
}

func TestMissingVersions(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": []
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "at least one version is required")
}

func TestCurrentVersionNotFound(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 99,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "current-version-id 99 not found")
}

func TestVersionReferencesUnknownSchema(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 99, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "version 1 references unknown schema-id 99")
}

func TestMissingSchemas(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "at least one schema is required")
}

func TestDuplicateDialects(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{
			"version-id": 1,
			"schema-id": 1,
			"timestamp-ms": 1234567890,
			"representations": [
				{"type": "sql", "sql": "SELECT 1", "dialect": "spark"},
				{"type": "sql", "sql": "SELECT 2", "dialect": "SPARK"}
			]
		}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "duplicate dialect")
}

func TestVersionLogNotChronological(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 2,
		"versions": [
			{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]},
			{"version-id": 2, "schema-id": 1, "timestamp-ms": 1234567900, "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "spark"}]}
		],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [
			{"timestamp-ms": 1234567900, "version-id": 2},
			{"timestamp-ms": 1234567890, "version-id": 1}
		]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "version log not in chronological order")
}

func TestVersionLogReferencesInvalidVersion(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [
			{"timestamp-ms": 1234567890, "version-id": 1},
			{"timestamp-ms": 1234567900, "version-id": 99}
		]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "version log entry at index 1 references non-existent version-id 99")
}

func TestPreValidateInitializesSlices(t *testing.T) {
	m := &metadata{
		UUID:             "test-uuid",
		FmtVersion:       1,
		Loc:              "s3://bucket/test",
		CurrentVersionId: 1,
	}

	m.preValidate()

	assert.NotNil(t, m.SchemaList)
	assert.NotNil(t, m.VersionList)
	assert.NotNil(t, m.VersionLogList)
	assert.NotNil(t, m.Props)

	assert.Empty(t, m.SchemaList)
	assert.Empty(t, m.VersionList)
	assert.Empty(t, m.VersionLogList)
	assert.Empty(t, m.Props)
}

func TestNilFieldsInJSON(t *testing.T) {
	validJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(validJSON), &meta)
	require.NoError(t, err)

	assert.NotNil(t, meta.Props)
	assert.Empty(t, meta.Props)
}

func TestMissingCurrentVersionID(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "current-version-id is required")
}

func TestInvalidCurrentVersionIDZero(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 0,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "current-version-id must be positive")
}

func TestMultipleVersionsValidation(t *testing.T) {
	validJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 2,
		"versions": [
			{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]},
			{"version-id": 2, "schema-id": 2, "timestamp-ms": 1234567900, "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "trino"}]}
		],
		"schemas": [
			{"schema-id": 1, "type": "struct", "fields": [{"id": 1, "name": "x", "required": false, "type": "long"}]},
			{"schema-id": 2, "type": "struct", "fields": [{"id": 1, "name": "y", "required": false, "type": "string"}]}
		],
		"version-log": [
			{"timestamp-ms": 1234567890, "version-id": 1},
			{"timestamp-ms": 1234567900, "version-id": 2}
		]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(validJSON), &meta)
	require.NoError(t, err)

	assert.Len(t, meta.VersionList, 2)
	assert.Len(t, meta.SchemaList, 2)
	assert.Len(t, meta.VersionLogList, 2)
}

func TestDirectValidateCall(t *testing.T) {
	m := &metadata{
		UUID:             "test-uuid",
		FmtVersion:       1,
		Loc:              "s3://bucket/test",
		CurrentVersionId: 1,
		VersionList: []Version{
			{
				VersionID:   1,
				SchemaID:    1,
				TimestampMs: 1234567890,
				Representations: []SQLRepresentation{
					{Type: "sql", SQL: "SELECT 1", Dialect: "spark"},
				},
			},
		},
		SchemaList: []*iceberg.Schema{
			iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: false},
			),
		},
		VersionLogList: []VersionLogEntry{
			{TimestampMs: 1234567890, VersionID: 1},
		},
	}

	m.preValidate()
	err := m.validate()
	require.NoError(t, err)
}
