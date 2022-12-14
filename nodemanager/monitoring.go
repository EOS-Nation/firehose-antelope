// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nodemanager

import (
	"context"
	"encoding/hex"
	"github.com/streamingfast/bstream"
	"time"

	"go.uber.org/zap"
)

// Monitor manages the 'readinessProbe' bool for healthz purposes and
// the stateos drift/headblock.
//
// This should be performed through a go routine.
func (s *NodeosSuperviser) Monitor() {
	var lastHeadBlockTime time.Time
	var lastDbSizeTime time.Time

	getInfoFailureCount := 0

	for {
		time.Sleep(5 * time.Second)
		if !s.IsRunning() {
			getInfoFailureCount = 0
			continue
		}

		chainInfo, err := s.api.GetInfo(context.Background())
		if err != nil {
			s.logger.Warn("got err on get into", zap.Error(err))
			getInfoFailureCount++
			continue
		}

		s.logger.Debug("got chain info", zap.Duration("delta", time.Since(lastHeadBlockTime)))
		getInfoFailureCount = 0
		s.chainID = chainInfo.ChainID
		s.serverVersion = chainInfo.ServerVersion
		s.serverVersionString = chainInfo.ServerVersionString
		s.lastBlockSeen = chainInfo.HeadBlockNum

		lastHeadBlockTime = chainInfo.HeadBlockTime.Time

		if s.headBlockUpdateFunc != nil {
			err := s.headBlockUpdateFunc(&bstream.Block{
				Id:        hex.EncodeToString(chainInfo.HeadBlockID),
				Number:    uint64(chainInfo.HeadBlockNum),
				Timestamp: chainInfo.HeadBlockTime.Time,
				LibNum:    uint64(chainInfo.LastIrreversibleBlockNum),
			})
			if err != nil {
				s.Logger.Error("failed to update head block", zap.Error(err))
			}
		}

		if lastDbSizeTime.IsZero() || time.Now().Sub(lastDbSizeTime).Seconds() > 30.0 {
			s.Logger.Debug("first monitoring call or more than 30s has elapsed since last call, querying db size from nodeos")
			dbSize, err := s.api.GetDBSize(context.Background())
			if err != nil {
				s.Logger.Info("unable to get db size", zap.Error(err))
				continue
			}

			lastDbSizeTime = time.Now()

			leapDbSizeInfo.SetFloat64(float64(dbSize.FreeBytes), "FreeBytes")
			leapDbSizeInfo.SetFloat64(float64(dbSize.UsedBytes), "UsedBytes")
			leapDbSizeInfo.SetFloat64(float64(dbSize.Size), "Size")
		}
	}
}
