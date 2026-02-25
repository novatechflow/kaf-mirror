// Copyright 2025 Scalytics, Inc. and Scalytics Europe, LTD
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"time"
)

type NavigationState int

const (
	TopLevelState NavigationState = iota
	CategoryViewState
	DetailViewState
)

type Category int

const (
	ClustersCategory Category = iota
	JobsCategory
	InsightsCategory
	ComplianceCategory
)

type PaneFocus int

const (
	ListPaneFocus PaneFocus = iota
	DetailPaneFocus
)

type NavigationContext struct {
	State       NavigationState
	Category    Category
	ItemID      string
	Breadcrumbs []string
	LastUpdate  time.Time
	PaneFocus   PaneFocus
}

func NewNavigationContext() *NavigationContext {
	return &NavigationContext{
		State:       TopLevelState,
		Category:    ClustersCategory,
		ItemID:      "",
		Breadcrumbs: []string{"Dashboard"},
		LastUpdate:  time.Now(),
	}
}

func (nc *NavigationContext) NavigateToCategory(category Category) {
	nc.State = CategoryViewState
	nc.Category = category
	nc.ItemID = ""
	nc.PaneFocus = ListPaneFocus // Default to list pane

	categoryNames := map[Category]string{
		ClustersCategory:   "Clusters",
		JobsCategory:       "Jobs",
		InsightsCategory:   "Insights",
		ComplianceCategory: "Compliance",
	}

	nc.Breadcrumbs = []string{"Dashboard", categoryNames[category]}
	nc.LastUpdate = time.Now()
}

func (nc *NavigationContext) NavigateToDetail(itemID, itemName string) {
	nc.State = DetailViewState
	nc.ItemID = itemID

	if len(nc.Breadcrumbs) == 2 {
		nc.Breadcrumbs = append(nc.Breadcrumbs, itemName)
	} else if len(nc.Breadcrumbs) >= 3 {
		nc.Breadcrumbs[2] = itemName
	}

	nc.LastUpdate = time.Now()
}

func (nc *NavigationContext) NavigateBack() {
	switch nc.State {
	case DetailViewState:
		nc.State = CategoryViewState
		nc.ItemID = ""
		if len(nc.Breadcrumbs) > 2 {
			nc.Breadcrumbs = nc.Breadcrumbs[:2]
		}
	case CategoryViewState:
		nc.State = TopLevelState
		nc.Breadcrumbs = []string{"Dashboard"}
	case TopLevelState:
	}
	nc.LastUpdate = time.Now()
}

func (nc *NavigationContext) GetBreadcrumbText() string {
	text := ""
	for i, crumb := range nc.Breadcrumbs {
		if i > 0 {
			text += " > "
		}
		text += crumb
	}
	return text
}

func (nc *NavigationContext) IsAtTopLevel() bool {
	return nc.State == TopLevelState
}

func (nc *NavigationContext) IsInCategory() bool {
	return nc.State == CategoryViewState
}

func (nc *NavigationContext) IsInDetail() bool {
	return nc.State == DetailViewState
}
