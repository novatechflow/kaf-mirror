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
	ui "github.com/gizak/termui/v3"
)

type EventRouter struct {
	context *NavigationContext
}

func NewEventRouter(context *NavigationContext) *EventRouter {
	return &EventRouter{
		context: context,
	}
}

func (er *EventRouter) RouteEvent(event ui.Event) EventAction {
	switch event.ID {
	case "q", "<C-c>":
		return ExitAction{}
	case "<Escape>", "b", "B":
		return BackAction{}
	}

	switch er.context.State {
	case TopLevelState:
		return er.routeTopLevelEvent(event)
	case CategoryViewState:
		return er.routeCategoryEvent(event)
	case DetailViewState:
		return er.routeDetailEvent(event)
	}

	return NoAction{}
}

func (er *EventRouter) routeTopLevelEvent(event ui.Event) EventAction {
	switch event.ID {
	case "1":
		return NavigateToCategoryAction{Category: ClustersCategory}
	case "2":
		return NavigateToCategoryAction{Category: JobsCategory}
	case "3":
		return NavigateToCategoryAction{Category: InsightsCategory}
	case "4":
		return NavigateToCategoryAction{Category: ComplianceCategory}
	case "<Down>", "j":
		return MoveFocusAction{Direction: Down}
	case "<Up>", "k":
		return MoveFocusAction{Direction: Up}
	case "<Right>", "l":
		return MoveFocusAction{Direction: Right}
	case "<Left>", "h":
		return MoveFocusAction{Direction: Left}
	case "<Enter>":
		return SelectItemAction{}
	}

	return NoAction{}
}

func (er *EventRouter) routeCategoryEvent(event ui.Event) EventAction {
	switch event.ID {
	case "<Down>", "j":
		return ScrollAction{Direction: Down}
	case "<Up>", "k":
		return ScrollAction{Direction: Up}
	case "<PageDown>":
		return ScrollAction{Direction: PageDown}
	case "<PageUp>":
		return ScrollAction{Direction: PageUp}
	case "<Enter>":
		return SelectItemAction{}
	case "<Tab>", "<Right>", "l":
		return SwitchPaneFocusAction{Direction: Right}
	case "<S-Tab>", "<Left>", "h":
		return SwitchPaneFocusAction{Direction: Left}
	case "r", "R":
		return RefreshAction{}
	}

	if er.context.Category == JobsCategory {
		switch event.ID {
		case "s", "S":
			return JobControlAction{Action: "start"}
		case "t", "T":
			return JobControlAction{Action: "stop"}
		case "p", "P":
			return JobControlAction{Action: "pause"}
		case "x", "X":
			return JobControlAction{Action: "restart"}
		}
	}

	return NoAction{}
}

func (er *EventRouter) routeDetailEvent(event ui.Event) EventAction {
	switch event.ID {
	case "<Down>", "j":
		return ScrollAction{Direction: Down}
	case "<Up>", "k":
		return ScrollAction{Direction: Up}
	case "<PageDown>":
		return ScrollAction{Direction: PageDown}
	case "<PageUp>":
		return ScrollAction{Direction: PageUp}
	case "r", "R":
		return RefreshAction{}
	}

	return NoAction{}
}

type Direction int

const (
	Up Direction = iota
	Down
	Left
	Right
	PageUp
	PageDown
)

type EventAction interface {
	ActionType() string
}

type NoAction struct{}

func (NoAction) ActionType() string { return "none" }

type ExitAction struct{}

func (ExitAction) ActionType() string { return "exit" }

type BackAction struct{}

func (BackAction) ActionType() string { return "back" }

type NavigateToCategoryAction struct {
	Category Category
}

func (NavigateToCategoryAction) ActionType() string { return "navigate_category" }

type MoveFocusAction struct {
	Direction Direction
}

func (MoveFocusAction) ActionType() string { return "move_focus" }

type ScrollAction struct {
	Direction Direction
}

func (ScrollAction) ActionType() string { return "scroll" }

type SelectItemAction struct{}

func (SelectItemAction) ActionType() string { return "select" }

type RefreshAction struct{}

func (RefreshAction) ActionType() string { return "refresh" }

type JobControlAction struct {
	Action string
}

func (JobControlAction) ActionType() string { return "job_control" }

type SwitchPaneFocusAction struct {
	Direction Direction
}

func (SwitchPaneFocusAction) ActionType() string { return "switch_pane" }
