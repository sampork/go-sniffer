// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"context"

	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/types"
	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/util/must"
	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/wire"
)

// MsgGetFreeMonitoringStatus implements `getFreeMonitoringStatus` command.
func (h *Handler) MsgGetFreeMonitoringStatus(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	state := h.StateProvider.Get().TelemetryString()
	message := "monitoring is " + state

	var reply wire.OpMsg
	must.NoError(reply.SetSections(wire.OpMsgSection{
		Documents: []*types.Document{must.NotFail(types.NewDocument(
			"state", state,
			"message", message,
			"ok", float64(1),
		))},
	}))

	return &reply, nil
}