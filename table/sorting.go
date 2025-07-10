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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
)

type SortDirection string

const (
	SortASC  SortDirection = "asc"
	SortDESC SortDirection = "desc"
)

type NullOrder string

const (
	NullsFirst NullOrder = "nulls-first"
	NullsLast  NullOrder = "nulls-last"
)

var (
	ErrInvalidSortDirection = errors.New("invalid sort direction, must be 'asc' or 'desc'")
	ErrInvalidNullOrder     = errors.New("invalid null order, must be 'nulls-first' or 'nulls-last'")
)

// SortField describes a field used in a sort order definition.
type SortField struct {
	// SourceID is the source column id from the table's schema
	SourceID int `json:"source-id"`
	// Transform is the tranformation used to produce values to be
	// sorted on from the source column.
	Transform iceberg.Transform `json:"transform"`
	// Direction is an enum indicating ascending or descending direction.
	Direction SortDirection `json:"direction"`
	// NullOrder describes the order of null values when sorting
	// should be only either nulls-first or nulls-last enum values.
	NullOrder NullOrder `json:"null-order"`
}

// Equals checks for equality with another SortField.
func (s *SortField) Equals(other SortField) bool {
	return s.SourceID == other.SourceID &&
		s.Direction == other.Direction &&
		s.NullOrder == other.NullOrder &&
		s.Transform == other.Transform
}

func (s *SortField) String() string {
	if _, ok := s.Transform.(iceberg.IdentityTransform); ok {
		return fmt.Sprintf("%d %s %s", s.SourceID, s.Direction, s.NullOrder)
	}

	return fmt.Sprintf("%s(%d) %s %s", s.Transform, s.SourceID, s.Direction, s.NullOrder)
}

func (s *SortField) MarshalJSON() ([]byte, error) {
	if s.Direction == "" {
		s.Direction = SortASC
	}

	if s.NullOrder == "" {
		if s.Direction == SortASC {
			s.NullOrder = NullsFirst
		} else {
			s.NullOrder = NullsLast
		}
	}

	type Alias SortField
	return json.Marshal(struct {
		Transform string `json:"transform"`
		*Alias
	}{
		Transform: s.Transform.String(),
		Alias:     (*Alias)(s),
	})
}

func (s *SortField) UnmarshalJSON(b []byte) error {
	type Alias SortField
	aux := struct {
		TransformString string `json:"transform"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	err := json.Unmarshal(b, &aux)
	if err != nil {
		return err
	}

	if s.Transform, err = iceberg.ParseTransform(aux.TransformString); err != nil {
		return err
	}

	switch s.Direction {
	case SortASC, SortDESC:
	default:
		return ErrInvalidSortDirection
	}

	switch s.NullOrder {
	case NullsFirst, NullsLast:
	default:
		return ErrInvalidNullOrder
	}

	return nil
}

const (
	InitialSortOrderID  = 1
	UnsortedSortOrderID = 0
)

// A default Sort Order indicating no sort order at all
var UnsortedSortOrder = SortOrder{OrderID: UnsortedSortOrderID, Fields: []SortField{}}

// SortOrder describes how the data is sorted within the table.
type SortOrder struct {
	OrderID int         `json:"order-id"`
	Fields  []SortField `json:"fields"`
}

// IsUnsorted returns true if the sort order has no fields.
func (s SortOrder) IsUnsorted() bool {
	return len(s.Fields) == 0
}

func (s SortOrder) Equals(rhs SortOrder) bool {
	if s.OrderID != rhs.OrderID || len(s.Fields) != len(rhs.Fields) {
		return false
	}
	for i := range s.Fields {
		if !s.Fields[i].Equals(rhs.Fields[i]) {
			return false
		}
	}
	return true
}

func (s SortOrder) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d: ", s.OrderID)
	b.WriteByte('[')
	for i, f := range s.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(f.String())
	}
	b.WriteByte(']')

	return b.String()
}

// Validate checks if the sort order is compatible with the given schema.
func (s *SortOrder) Validate(schema *iceberg.Schema) error {
	for _, sortField := range s.Fields {
		_, ok := schema.FindFieldByID(sortField.SourceID)
		if !ok {
			return fmt.Errorf("cannot find source column for sort field: %s", sortField.String())
		}

		// FIXME: add missing validations
		//if !sourceField.Type.IsPrimitive() {
		//	return fmt.Errorf("cannot sort by non-primitive source field: %s", sourceField.Type)
		//}

		//if _, err := sortField.Transform.ResultType(sourceField.Type); err != nil {
		//	return fmt.Errorf("invalid source type %s for transform %s", sourceField.Type, sortField.Transform)
		//}
	}
	return nil
}

func (s *SortOrder) UnmarshalJSON(b []byte) error {
	type Alias SortOrder
	aux := (*Alias)(s)

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	if len(s.Fields) == 0 {
		s.Fields = []SortField{}
		if s.OrderID != 0 {
			return errors.New("unsorted order ID must be 0")
		}
	}

	if s.OrderID == 0 && len(s.Fields) > 0 {
		return errors.New("sort order ID 0 is reserved for unsorted order")
	}

	return nil
}

// AssignFreshSortOrderIDs updates and reassigns the field source IDs from the old schema
// to the corresponding fields in the fresh schema.
func AssignFreshSortOrderIDs(sortOrder SortOrder, old, fresh *iceberg.Schema) (SortOrder, error) {
	orderID := InitialSortOrderID
	if sortOrder.IsUnsorted() {
		orderID = UnsortedSortOrderID
	}
	return AssignFreshSortOrderIDsWithID(sortOrder, old, fresh, orderID)
}

// AssignFreshSortOrderIDsWithID is like AssignFreshSortOrderIDs but allows specifying the id.
func AssignFreshSortOrderIDsWithID(sortOrder SortOrder, old, fresh *iceberg.Schema, sortOrderID int) (SortOrder, error) {
	if sortOrder.IsUnsorted() {
		return UnsortedSortOrder, nil
	}

	fields := make([]SortField, 0, len(sortOrder.Fields))
	for _, field := range sortOrder.Fields {
		originalField, ok := old.FindColumnName(field.SourceID)
		if !ok {
			return SortOrder{}, fmt.Errorf("cannot find source column id %s in old schema", field.String())
		}
		freshField, ok := fresh.FindFieldByName(originalField)
		if !ok {
			return SortOrder{}, fmt.Errorf("cannot find field %s in fresh schema", originalField)
		}

		fields = append(fields, SortField{
			SourceID:  freshField.ID,
			Transform: field.Transform,
			Direction: field.Direction,
			NullOrder: field.NullOrder,
		})
	}

	return SortOrder{OrderID: sortOrderID, Fields: fields}, nil
}
