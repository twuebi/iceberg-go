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

package iceberg

import (
	"encoding/json"
	"fmt"
	"iter"
	"net/url"
	"path"
	"slices"
	"strings"
)

const (
	partitionDataIDStart   = 1000
	InitialPartitionSpecID = 0
)

// UnpartitionedSpec is the default unpartitioned spec which can
// be used for comparisons or to just provide a convenience for referencing
// the same unpartitioned spec object.
var UnpartitionedSpec = &PartitionSpec{id: 0}

// PartitionField represents how one partition value is derived from the
// source column by transformation.
type PartitionField struct {
	// SourceID is the source column id of the table's schema
	SourceID int `json:"source-id"`
	// FieldID is the partition field id across all the table partition specs
	FieldID int `json:"field-id"`
	// Name is the name of the partition field itself
	Name string `json:"name"`
	// Transform is the transform used to produce the partition value
	Transform Transform `json:"transform"`
}

func (p *PartitionField) String() string {
	return fmt.Sprintf("%d: %s: %s(%d)", p.FieldID, p.Name, p.Transform, p.SourceID)
}

func (p *PartitionField) UnmarshalJSON(b []byte) error {
	type Alias PartitionField
	aux := struct {
		TransformString string `json:"transform"`
		*Alias
	}{
		Alias: (*Alias)(p),
	}

	err := json.Unmarshal(b, &aux)
	if err != nil {
		return err
	}

	if p.Transform, err = ParseTransform(aux.TransformString); err != nil {
		return err
	}

	return nil
}

// PartitionSpec captures the transformation from table data to partition values
type PartitionSpec struct {
	// any change to a PartitionSpec will produce a new spec id
	id     int
	fields []PartitionField

	// this is populated by initialize after creation
	sourceIdToFields map[int][]PartitionField
}

func NewPartitionSpec(fields ...PartitionField) PartitionSpec {
	return NewPartitionSpecID(InitialPartitionSpecID, fields...)
}

func NewPartitionSpecID(id int, fields ...PartitionField) PartitionSpec {
	ret := PartitionSpec{id: id, fields: fields}
	ret.initialize()

	return ret
}

// CompatibleWith returns true if this partition spec is considered
// compatible with the passed in partition spec. This means that the two
// specs have equivalent field lists regardless of the spec id.
func (ps *PartitionSpec) CompatibleWith(other *PartitionSpec) bool {
	if ps == other {
		return true
	}

	if len(ps.fields) != len(other.fields) {
		return false
	}

	return slices.EqualFunc(ps.fields, other.fields, func(left, right PartitionField) bool {
		return left.SourceID == right.SourceID && left.Name == right.Name &&
			left.Transform == right.Transform
	})
}

// Equals returns true iff the field lists are the same AND the spec id
// is the same between this partition spec and the provided one.
func (ps PartitionSpec) Equals(other PartitionSpec) bool {
	if ps.id != other.id || len(ps.fields) != len(other.fields) {
		return false
	}

	for i := range ps.fields {
		if ps.fields[i].SourceID != other.fields[i].SourceID ||
			ps.fields[i].FieldID != other.fields[i].FieldID ||
			ps.fields[i].Name != other.fields[i].Name ||
			ps.fields[i].Transform != other.fields[i].Transform {
			return false
		}
	}
	return true
}

// Fields returns a clone of the partition fields in this spec.
func (ps *PartitionSpec) Fields() iter.Seq[PartitionField] {
	return slices.Values(ps.fields)
}

func (ps PartitionSpec) MarshalJSON() ([]byte, error) {
	if ps.fields == nil {
		ps.fields = []PartitionField{}
	}

	return json.Marshal(struct {
		ID     int              `json:"spec-id"`
		Fields []PartitionField `json:"fields"`
	}{ps.id, ps.fields})
}

func (ps *PartitionSpec) UnmarshalJSON(b []byte) error {
	aux := struct {
		ID     int              `json:"spec-id"`
		Fields []PartitionField `json:"fields"`
	}{ID: ps.id, Fields: ps.fields}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	ps.id, ps.fields = aux.ID, aux.Fields
	ps.initialize()

	return nil
}

func (ps *PartitionSpec) initialize() {
	ps.sourceIdToFields = make(map[int][]PartitionField)
	for _, f := range ps.fields {
		ps.sourceIdToFields[f.SourceID] = append(ps.sourceIdToFields[f.SourceID], f)
	}
}

func (ps *PartitionSpec) ID() int                    { return ps.id }
func (ps *PartitionSpec) NumFields() int             { return len(ps.fields) }
func (ps *PartitionSpec) Field(i int) PartitionField { return ps.fields[i] }

func (ps PartitionSpec) IsUnpartitioned() bool {
	if len(ps.fields) == 0 {
		return true
	}

	for _, f := range ps.fields {
		if _, ok := f.Transform.(VoidTransform); !ok {
			return false
		}
	}

	return true
}

// IsSequential checks if partition field IDs are sequential, starting from 1000.
func (ps *PartitionSpec) IsSequential() bool {
	for i, f := range ps.fields {
		if f.FieldID != partitionDataIDStart+i {
			return false
		}
	}
	return true
}

func (ps *PartitionSpec) FieldsBySourceID(fieldID int) []PartitionField {
	return slices.Clone(ps.sourceIdToFields[fieldID])
}

func (ps PartitionSpec) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range ps.fields {
		if i == 0 {
			b.WriteString("\n")
		}
		b.WriteString("\t")
		b.WriteString(f.String())
		b.WriteString("\n")
	}
	b.WriteByte(']')

	return b.String()
}

// LastAssignedFieldID returns the highest field ID in the partition spec.
func (ps *PartitionSpec) LastAssignedFieldID() int {
	if len(ps.fields) == 0 {
		return partitionDataIDStart - 1
	}

	id := ps.fields[0].FieldID
	for _, f := range ps.fields[1:] {
		if f.FieldID > id {
			id = f.FieldID
		}
	}

	return id
}

// PartitionType produces a struct of the partition spec, returning an error if incompatible.
func (ps *PartitionSpec) PartitionType(schema *Schema) (*StructType, error) {
	nestedFields := make([]NestedField, 0, len(ps.fields))
	for _, field := range ps.fields {
		sourceField, ok := schema.FindFieldByID(field.SourceID)
		if !ok {
			return nil, fmt.Errorf("cannot find source column for partition field: %d", field.SourceID)
		}

		sourceType := sourceField.Type
		resultType := field.Transform.ResultType(sourceType)
		// FIXME: validation of transform is missing
		//if err != nil {
		//	return nil, fmt.Errorf("invalid source type %s for transform %s",
		//		sourceType, field.Transform)
		//}

		nestedFields = append(nestedFields, NestedField{
			ID:       field.FieldID,
			Name:     field.Name,
			Type:     resultType,
			Required: false,
		})
	}

	return &StructType{FieldList: nestedFields}, nil
}

// PartitionToPath produces a proper partition path from the data and schema by
// converting the values to human readable strings and properly escaping.
func (ps *PartitionSpec) PartitionToPath(data structLike, sc *Schema) string {
	partType, err := ps.PartitionType(sc)
	if err != nil {
		return ""
	}

	segments := make([]string, 0, len(partType.FieldList))
	for i := range partType.Fields() {
		valueStr := ps.fields[i].Transform.ToHumanStr(data.Get(i))

		segments = append(segments, fmt.Sprintf("%s=%s",
			url.QueryEscape(ps.fields[i].Name), url.QueryEscape(valueStr)))
	}

	return path.Join(segments...)
}

// BindPartitionSpec creates a bound PartitionSpec from an unbound one by assigning field IDs.
func BindPartitionSpec(schema *Schema, spec *PartitionSpec, lastAssignedFieldID int) (*PartitionSpec, error) {
	newFields := make([]PartitionField, 0, len(spec.fields))
	nextFieldID := lastAssignedFieldID + 1

	for _, field := range spec.fields {
		_, ok := schema.FindFieldByID(field.SourceID)
		if !ok {
			return nil, fmt.Errorf("cannot find source field with id %d in schema", field.SourceID)
		}
		// FIXME: add validation
		//if _, err := field.Transform.ResultType(sourceField.Type); err != nil {
		//	return nil, fmt.Errorf("transform %s is not compatible with source type %s", field.Transform, sourceField.Type)
		//}

		newField := field
		if newField.FieldID < partitionDataIDStart {
			newField.FieldID = nextFieldID
			nextFieldID++
		}
		newFields = append(newFields, newField)
	}

	newSpec := NewPartitionSpecID(spec.id, newFields...)
	return &newSpec, nil
}

// AssignFreshPartitionSpecIDs creates a new PartitionSpec by reassigning the field IDs
// from the old schema to the corresponding fields in the fresh schema.
func AssignFreshPartitionSpecIDs(spec *PartitionSpec, old, fresh *Schema) (PartitionSpec, error) {
	if spec == nil {
		return PartitionSpec{}, nil
	}

	newFields := make([]PartitionField, 0, len(spec.fields))
	for pos, field := range spec.fields {
		origCol, ok := old.FindColumnName(field.SourceID)
		if !ok {
			return PartitionSpec{}, fmt.Errorf("could not find field in old schema: %s", field.Name)
		}

		freshField, ok := fresh.FindFieldByName(origCol)
		if !ok {
			return PartitionSpec{}, fmt.Errorf("could not find field in fresh schema: %s", field.Name)
		}

		newFields = append(newFields, PartitionField{
			Name:      field.Name,
			SourceID:  freshField.ID,
			FieldID:   partitionDataIDStart + pos,
			Transform: field.Transform,
		})
	}

	return NewPartitionSpec(newFields...), nil
}

// GeneratePartitionFieldName returns default partition field name based on field transform type
func GeneratePartitionFieldName(schema *Schema, field PartitionField) (string, error) {
	if len(field.Name) > 0 {
		return field.Name, nil
	}

	sourceName, exists := schema.FindColumnName(field.SourceID)
	if !exists {
		return "", fmt.Errorf("could not find field with id %d", field.SourceID)
	}

	transform := field.Transform
	switch t := transform.(type) {
	case IdentityTransform:
		return sourceName, nil
	case VoidTransform:
		return sourceName + "_null", nil
	case BucketTransform:
		return fmt.Sprintf("%s_bucket_%d", sourceName, t.NumBuckets), nil
	case TruncateTransform:
		return fmt.Sprintf("%s_trunc_%d", sourceName, t.Width), nil
	default:
		return sourceName + "_" + t.String(), nil
	}
}
