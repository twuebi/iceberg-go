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

import "github.com/apache/iceberg-go"

const (
	UpdateActionAssignUUID            = "assign-uuid"
	UpdateActionUpgradeFormatVersion  = "upgrade-format-version"
	UpdateActionAddSchema             = "add-schema"
	UpdateActionSetLocation           = "set-location"
	UpdateActionSetProperties         = "set-properties"
	UpdateActionRemoveProperties      = "remove-properties"
	UpdateActionAddViewVersion        = "add-view-version"
	UpdateActionSetCurrentViewVersion = "set-current-view-version"
)

// ViewUpdate represents a change to view metadata.
type ViewUpdate interface {
	Action() string
	Apply(*MetadataBuilder) error
}

// AssignUUIDUpdate assigns a UUID to the view.
type AssignUUIDUpdate struct {
	UUID string `json:"uuid"`
}

func (u *AssignUUIDUpdate) Action() string {
	return UpdateActionAssignUUID
}

func (u *AssignUUIDUpdate) Apply(b *MetadataBuilder) error {
	return b.AssignUUID(u.UUID)
}

func NewAssignUUIDUpdate(uuid string) *AssignUUIDUpdate {
	return &AssignUUIDUpdate{UUID: uuid}
}

// UpgradeFormatVersionUpdate upgrades the format version.
type UpgradeFormatVersionUpdate struct {
	FormatVersion int `json:"format-version"`
}

func (u *UpgradeFormatVersionUpdate) Action() string {
	return UpdateActionUpgradeFormatVersion
}

func (u *UpgradeFormatVersionUpdate) Apply(b *MetadataBuilder) error {
	return b.UpgradeFormatVersion(u.FormatVersion)
}

func NewUpgradeFormatVersionUpdate(version int) *UpgradeFormatVersionUpdate {
	return &UpgradeFormatVersionUpdate{FormatVersion: version}
}

// AddSchemaUpdate adds a new schema.
type AddSchemaUpdate struct {
	Schema       *iceberg.Schema `json:"schema"`
	LastColumnID *int            `json:"last-column-id,omitempty"`
}

func (u *AddSchemaUpdate) Action() string {
	return UpdateActionAddSchema
}

func (u *AddSchemaUpdate) Apply(b *MetadataBuilder) error {
	return b.AddSchema(u.Schema)
}

func NewAddSchemaUpdate(schema *iceberg.Schema) *AddSchemaUpdate {
	return &AddSchemaUpdate{Schema: schema}
}

// SetLocationUpdate updates the view location.
type SetLocationUpdate struct {
	Location string `json:"location"`
}

func (u *SetLocationUpdate) Action() string {
	return UpdateActionSetLocation
}

func (u *SetLocationUpdate) Apply(b *MetadataBuilder) error {
	return b.SetLocation(u.Location)
}

func NewSetLocationUpdate(location string) *SetLocationUpdate {
	return &SetLocationUpdate{Location: location}
}

// SetPropertiesUpdate sets view properties.
type SetPropertiesUpdate struct {
	Updates map[string]string `json:"updates"`
}

func (u *SetPropertiesUpdate) Action() string {
	return UpdateActionSetProperties
}

func (u *SetPropertiesUpdate) Apply(b *MetadataBuilder) error {
	return b.SetProperties(u.Updates)
}

func NewSetPropertiesUpdate(updates map[string]string) *SetPropertiesUpdate {
	return &SetPropertiesUpdate{Updates: updates}
}

// RemovePropertiesUpdate removes view properties.
type RemovePropertiesUpdate struct {
	Removals []string `json:"removals"`
}

func (u *RemovePropertiesUpdate) Action() string {
	return UpdateActionRemoveProperties
}

func (u *RemovePropertiesUpdate) Apply(b *MetadataBuilder) error {
	return b.RemoveProperties(u.Removals)
}

func NewRemovePropertiesUpdate(removals []string) *RemovePropertiesUpdate {
	return &RemovePropertiesUpdate{Removals: removals}
}

// AddViewVersionUpdate adds a new view version.
type AddViewVersionUpdate struct {
	ViewVersion *Version `json:"view-version"`
}

func (u *AddViewVersionUpdate) Action() string {
	return UpdateActionAddViewVersion
}

func (u *AddViewVersionUpdate) Apply(b *MetadataBuilder) error {
	return b.AddVersion(u.ViewVersion)
}

func NewAddViewVersionUpdate(version *Version) *AddViewVersionUpdate {
	return &AddViewVersionUpdate{ViewVersion: version}
}

// SetCurrentViewVersionUpdate sets the current view version.
// VersionID can be -1 to reference the last added version.
type SetCurrentViewVersionUpdate struct {
	VersionID int64 `json:"version-id"`
}

func (u *SetCurrentViewVersionUpdate) Action() string {
	return UpdateActionSetCurrentViewVersion
}

func (u *SetCurrentViewVersionUpdate) Apply(b *MetadataBuilder) error {
	return b.SetCurrentVersionID(u.VersionID)
}

func NewSetCurrentViewVersionUpdate(versionID int64) *SetCurrentViewVersionUpdate {
	return &SetCurrentViewVersionUpdate{VersionID: versionID}
}
