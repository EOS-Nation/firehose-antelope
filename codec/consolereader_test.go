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

package codec

import (
	"bytes"
	"errors"
	"fmt"
	antelope_v3_1 "github.com/EOS-Nation/firehose-antelope/codec/antelope/v3.1"
	"github.com/EOS-Nation/firehose-antelope/types"
	"github.com/EOS-Nation/firehose-antelope/types/pb/sf/antelope/type/v1"
	"github.com/andreyvit/diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

// todo delete legacy benchmark replaced with console_reader_bench_test.go
//func TestConsoleReaderPerformances(t *testing.T) {
//	dmlogBenchmarkFile := os.Getenv("PERF_DMLOG_BENCHMARK_FILE")
//	if dmlogBenchmarkFile == "" || !fileExists(dmlogBenchmarkFile) {
//		t.Skipf("Environment variable 'PERF_DMLOG_BENCHMARK_FILE' not set or value %q is not an existing file", dmlogBenchmarkFile)
//		return
//	}
//
//	go func() {
//		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
//			zlogTest.Info("listening localhost:6060", zap.Error(err))
//		}
//	}()
//
//	fl, err := os.Open(dmlogBenchmarkFile)
//	require.NoError(t, err)
//
//	r, err := NewConsoleReader(fl)
//	require.NoError(t, err)
//	defer r.Close()
//
//	count := 1999
//
//	t0 := time.Now()
//
//	for i := 0; i < count; i++ {
//		blki, err := r.Read()
//		require.NoError(t, err)
//
//		blk := blki.(*pbantelope.Block)
//		fmt.Fprintln(os.Stderr, "Processing block", blk.Num())
//	}
//
//	d1 := time.Since(t0)
//	perSec := float64(count) / (float64(d1) / float64(time.Second))
//	fmt.Printf("%d blocks in %s (%f blocks/sec)", count, d1, perSec)
//}

func TestParseFromFile(t *testing.T) {

	tests := []struct {
		name         string
		deepMindFile string
		includeBlock func(block *pbantelope.Block) bool
		// readerOptions []ConsoleReaderOption
	}{
		// {"full", "testdata/deep-mind.dmlog", nil /*nil*/},
		{"full-3.1.x", "testdata/deep-mind-3.1.x.dmlog", nil /*nil*/},
		// {"max-console-log", "testdata/deep-mind.dmlog", blockWithConsole /*[]ConsoleReaderOption{LimitConsoleLength(10)}*/},
	}

	for _, test := range tests {
		t.Run(strings.Replace(test.deepMindFile, "testdata/", "", 1), func(t *testing.T) {
			// todo check if we need to test for expected panics
			//defer func() {
			//	if r := recover(); r != nil {
			//		require.Equal(t, test.expectedPanicErr, r, "Panicked with %s", r)
			//	}
			//}()

			cr := testFileConsoleReader(t, test.deepMindFile)

			var reader ObjectReader = func() (interface{}, error) {
				out, err := cr.ReadBlock()
				if err != nil {
					return nil, err
				}

				return out.ToProtocol().(*pbantelope.Block), nil
			}

			buf := &bytes.Buffer{}
			buf.Write([]byte("["))
			first := true

			for {
				out, err := reader()
				if v, ok := out.(proto.Message); ok && !isNil(v) {
					if !first {
						buf.Write([]byte(","))
					}
					first = false

					value, err := MarshalIndentToString(v, "  ")
					require.NoError(t, err)

					buf.Write([]byte(value))
				}

				if err == io.EOF {
					break
				}

				if len(buf.Bytes()) != 0 {
					buf.Write([]byte("\n"))
				}

				//if test.expectedErr == nil {
				//	require.NoError(t, err)
				//} else if err != nil {
				//	require.Equal(t, test.expectedErr, err)
				//	return
				//}
				require.NoError(t, err)
			}
			buf.Write([]byte("]"))

			goldenFile := test.deepMindFile + ".golden.json"
			if os.Getenv("GOLDEN_UPDATE") == "true" {
				err := os.WriteFile(goldenFile, buf.Bytes(), os.ModePerm)
				require.NoError(t, err)
			}

			cnt, err := os.ReadFile(goldenFile)
			require.NoError(t, err)

			if !assert.JSONEq(t, string(cnt), buf.String()) {
				t.Error("previous diff:\n" + unifiedDiff(t, cnt, buf.Bytes()))
			}
		})
	}
}

func unifiedDiff(t *testing.T, cnt1, cnt2 []byte) string {
	file1 := "/tmp/gotests-linediff-1"
	file2 := "/tmp/gotests-linediff-2"
	err := os.WriteFile(file1, cnt1, 0600)
	require.NoError(t, err)

	err = os.WriteFile(file2, cnt2, 0600)
	require.NoError(t, err)

	cmd := exec.Command("diff", "-u", file1, file2)
	out, _ := cmd.Output()

	return string(out)
}

func TestGeneratePBBlocks(t *testing.T) {
	t.Skip("generate only when deep-mind-3.1.x.dmlog changes")

	cr := testFileConsoleReader(t, "testdata/deep-mind-3.1.x.dmlog")

	for {
		out, err := cr.ReadBlock()
		if out != nil {
			block := out.ToProtocol().(*pbantelope.Block)

			bstreamBlock, err := types.BlockFromProto(block)
			require.NoError(t, err)

			pbBlock, err := bstreamBlock.ToProto()
			require.NoError(t, err)

			outputFile, err := os.Create(fmt.Sprintf("testdata/pbblocks/battlefield-block.%d.pb", block.Number))
			require.NoError(t, err)

			pbBlockBytes, err := proto.Marshal(pbBlock)
			require.NoError(t, err)

			_, err = outputFile.Write(pbBlockBytes)
			require.NoError(t, err)

			outputFile.Close()
		}

		if err == io.EOF {
			break
		}

		require.NoError(t, err)
	}
}

func testFileConsoleReader(t *testing.T, filename string) *ConsoleReader {
	t.Helper()

	fl, err := os.Open(filename)
	require.NoError(t, err)

	// todo use this if you want A LOT of logging
	// cr := testReaderConsoleReader(t.Helper, make(chan string, 10000), func() { fl.Close() }, zaptest.NewLogger(t))
	cr := testReaderConsoleReader(t.Helper, make(chan string, 10000), func() { fl.Close() }, nil)

	go cr.ProcessData(fl)

	return cr
}

func testReaderConsoleReader(helperFunc func(), lines chan string, closer func(), logger *zap.Logger) *ConsoleReader {

	l := &ConsoleReader{
		lines: lines,
		close: closer,
		ctx: &parseCtx{
			logger:       zlogTest,
			globalStats:  newConsoleReaderStats(),
			currentBlock: &pbantelope.Block{},
			currentTrace: &pbantelope.TransactionTrace{},
			abiDecoder:   newABIDecoder(true),
		},
		logger: zlogTest,
	}

	if logger != nil {
		l.logger = logger
	}

	return l
}

func Test_BlockRlimitOp(t *testing.T) {
	tests := []struct {
		line        string
		expected    *pbantelope.RlimitOp
		expectedErr error
	}{
		{
			`RLIMIT_OP CONFIG INS {"cpu_limit_parameters":{"target":20000,"max":200000,"periods":120,"max_multiplier":1000,"contract_rate":{"numerator":99,"denominator":100},"expand_rate":{"numerator":1000,"denominator":999}},"net_limit_parameters":{"target":104857,"max":1048576,"periods":120,"max_multiplier":1000,"contract_rate":{"numerator":99,"denominator":100},"expand_rate":{"numerator":1000,"denominator":999}},"account_cpu_usage_average_window":172800,"account_net_usage_average_window":172800}`,
			&pbantelope.RlimitOp{
				Operation: pbantelope.RlimitOp_OPERATION_INSERT,
				Kind: &pbantelope.RlimitOp_Config{
					Config: &pbantelope.RlimitConfig{
						CpuLimitParameters: &pbantelope.ElasticLimitParameters{
							Target:        20000,
							Max:           200000,
							Periods:       120,
							MaxMultiplier: 1000,
							ContractRate: &pbantelope.Ratio{
								Numerator:   99,
								Denominator: 100,
							},
							ExpandRate: &pbantelope.Ratio{
								Numerator:   1000,
								Denominator: 999,
							},
						},
						NetLimitParameters: &pbantelope.ElasticLimitParameters{
							Target:        104857,
							Max:           1048576,
							Periods:       120,
							MaxMultiplier: 1000,
							ContractRate: &pbantelope.Ratio{
								Numerator:   99,
								Denominator: 100,
							},
							ExpandRate: &pbantelope.Ratio{
								Numerator:   1000,
								Denominator: 999,
							},
						},
						AccountCpuUsageAverageWindow: 172800,
						AccountNetUsageAverageWindow: 172800,
					},
				},
			},
			nil,
		},
		{
			`RLIMIT_OP STATE INS {"average_block_net_usage":{"last_ordinal":1,"value_ex":2,"consumed":3},"average_block_cpu_usage":{"last_ordinal":4,"value_ex":5,"consumed":6},"pending_net_usage":7,"pending_cpu_usage":8,"total_net_weight":9,"total_cpu_weight":10,"total_ram_bytes":11,"virtual_net_limit":1048576,"virtual_cpu_limit":200000}`,
			&pbantelope.RlimitOp{
				Operation: pbantelope.RlimitOp_OPERATION_INSERT,
				Kind: &pbantelope.RlimitOp_State{
					State: &pbantelope.RlimitState{
						AverageBlockNetUsage: &pbantelope.UsageAccumulator{
							LastOrdinal: 1,
							ValueEx:     2,
							Consumed:    3,
						},
						AverageBlockCpuUsage: &pbantelope.UsageAccumulator{
							LastOrdinal: 4,
							ValueEx:     5,
							Consumed:    6,
						},
						PendingNetUsage: 7,
						PendingCpuUsage: 8,
						TotalNetWeight:  9,
						TotalCpuWeight:  10,
						TotalRamBytes:   11,
						VirtualNetLimit: 1048576,
						VirtualCpuLimit: 200000,
					},
				},
			},
			nil,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ctx := newParseCtx()
			err := ctx.readRlimitOp(test.line)

			require.Equal(t, test.expectedErr, err)

			if test.expectedErr == nil {
				require.Len(t, ctx.currentBlock.RlimitOps, 1)

				expected := protoJSONMarshalIndent(t, test.expected)
				actual := protoJSONMarshalIndent(t, ctx.currentBlock.RlimitOps[0])

				assert.JSONEq(t, expected, actual, diff.LineDiff(expected, actual))
			}
		})
	}
}

func Test_TraceRlimitOp(t *testing.T) {
	tests := []struct {
		line        string
		expected    *pbantelope.RlimitOp
		expectedErr error
	}{
		{
			`RLIMIT_OP ACCOUNT_LIMITS INS {"owner":"eosio.ram","net_weight":-1,"cpu_weight":-1,"ram_bytes":-1}`,
			&pbantelope.RlimitOp{
				Operation: pbantelope.RlimitOp_OPERATION_INSERT,
				Kind: &pbantelope.RlimitOp_AccountLimits{
					AccountLimits: &pbantelope.RlimitAccountLimits{
						Owner:     "eosio.ram",
						NetWeight: -1,
						CpuWeight: -1,
						RamBytes:  -1,
					},
				},
			},
			nil,
		},
		{
			`RLIMIT_OP ACCOUNT_USAGE UPD {"owner":"eosio","net_usage":{"last_ordinal":0,"value_ex":868696,"consumed":1},"cpu_usage":{"last_ordinal":0,"value_ex":572949,"consumed":101},"ram_usage":1181072}`,
			&pbantelope.RlimitOp{
				Operation: pbantelope.RlimitOp_OPERATION_UPDATE,
				Kind: &pbantelope.RlimitOp_AccountUsage{
					AccountUsage: &pbantelope.RlimitAccountUsage{
						Owner:    "eosio",
						NetUsage: &pbantelope.UsageAccumulator{LastOrdinal: 0, ValueEx: 868696, Consumed: 1},
						CpuUsage: &pbantelope.UsageAccumulator{LastOrdinal: 0, ValueEx: 572949, Consumed: 101},
						RamUsage: 1181072,
					},
				},
			},
			nil,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ctx := newParseCtx()
			err := ctx.readRlimitOp(test.line)

			require.Equal(t, test.expectedErr, err)

			if test.expectedErr == nil {
				require.Len(t, ctx.currentTrace.RlimitOps, 1)

				expected := protoJSONMarshalIndent(t, test.expected)
				actual := protoJSONMarshalIndent(t, ctx.currentTrace.RlimitOps[0])

				assert.JSONEq(t, expected, actual, diff.LineDiff(expected, actual))
			}
		})
	}
}

func Test_readPermOp(t *testing.T) {
	auth := &pbantelope.Authority{
		Threshold: 1,
		Accounts: []*pbantelope.PermissionLevelWeight{
			{
				Permission: &pbantelope.PermissionLevel{Actor: "eosio", Permission: "active"},
				Weight:     1,
			},
		},
	}

	tests := []struct {
		line        string
		expected    *pbantelope.PermOp
		expectedErr error
	}{
		{
			`PERM_OP INS 0 {"parent":1,"owner":"eosio.ins","name":"prod.major","last_updated":"2018-06-08T08:08:08.888","auth":{"threshold":1,"keys":[],"accounts":[{"permission":{"actor":"eosio","permission":"active"},"weight":1}],"waits":[]}}`,
			&pbantelope.PermOp{
				Operation:   pbantelope.PermOp_OPERATION_INSERT,
				ActionIndex: 0,
				OldPerm:     nil,
				NewPerm: &pbantelope.PermissionObject{
					ParentId:    1,
					Owner:       "eosio.ins",
					Name:        "prod.major",
					LastUpdated: timestamppb.New(mustTimeParse("2018-06-08T08:08:08.888")),
					Authority:   auth,
				},
			},
			nil,
		},
		{
			`PERM_OP UPD 0 {"old":{"parent":2,"owner":"eosio.old","name":"prod.major","last_updated":"2018-06-08T08:08:08.888","auth":{"threshold":1,"keys":[],"accounts":[{"permission":{"actor":"eosio","permission":"active"},"weight":1}],"waits":[]}},"new":{"parent":3,"owner":"eosio.new","name":"prod.major","last_updated":"2018-06-08T08:08:08.888","auth":{"threshold":1,"keys":[],"accounts":[{"permission":{"actor":"eosio","permission":"active"},"weight":1}],"waits":[]}}}`,
			&pbantelope.PermOp{
				Operation:   pbantelope.PermOp_OPERATION_UPDATE,
				ActionIndex: 0,
				OldPerm: &pbantelope.PermissionObject{
					ParentId:    2,
					Owner:       "eosio.old",
					Name:        "prod.major",
					LastUpdated: timestamppb.New(mustTimeParse("2018-06-08T08:08:08.888")),
					Authority:   auth,
				},
				NewPerm: &pbantelope.PermissionObject{
					ParentId:    3,
					Owner:       "eosio.new",
					Name:        "prod.major",
					LastUpdated: timestamppb.New(mustTimeParse("2018-06-08T08:08:08.888")),
					Authority:   auth,
				},
			},
			nil,
		},
		{
			`PERM_OP REM 0 {"parent":4,"owner":"eosio.rem","name":"prod.major","last_updated":"2018-06-08T08:08:08.888","auth":{"threshold":1,"keys":[],"accounts":[{"permission":{"actor":"eosio","permission":"active"},"weight":1}],"waits":[]}}`,
			&pbantelope.PermOp{
				Operation:   pbantelope.PermOp_OPERATION_REMOVE,
				ActionIndex: 0,
				OldPerm: &pbantelope.PermissionObject{
					ParentId:    4,
					Owner:       "eosio.rem",
					Name:        "prod.major",
					LastUpdated: timestamppb.New(mustTimeParse("2018-06-08T08:08:08.888")),
					Authority:   auth,
				},
				NewPerm: nil,
			},
			nil,
		},

		// New format
		{
			`PERM_OP INS 0 2 {"parent":1,"owner":"eosio.ins","name":"prod.major","last_updated":"2018-06-08T08:08:08.888","auth":{"threshold":1,"keys":[],"accounts":[{"permission":{"actor":"eosio","permission":"active"},"weight":1}],"waits":[]}}`,
			&pbantelope.PermOp{
				Operation:   pbantelope.PermOp_OPERATION_INSERT,
				ActionIndex: 0,
				OldPerm:     nil,
				NewPerm: &pbantelope.PermissionObject{
					Id:          2,
					ParentId:    1,
					Owner:       "eosio.ins",
					Name:        "prod.major",
					LastUpdated: timestamppb.New(mustTimeParse("2018-06-08T08:08:08.888")),
					Authority:   auth,
				},
			},
			nil,
		},
		{
			`PERM_OP UPD 0 4 {"old":{"parent":2,"owner":"eosio.old","name":"prod.major","last_updated":"2018-06-08T08:08:08.888","auth":{"threshold":1,"keys":[],"accounts":[{"permission":{"actor":"eosio","permission":"active"},"weight":1}],"waits":[]}},"new":{"parent":3,"owner":"eosio.new","name":"prod.major","last_updated":"2018-06-08T08:08:08.888","auth":{"threshold":1,"keys":[],"accounts":[{"permission":{"actor":"eosio","permission":"active"},"weight":1}],"waits":[]}}}`,
			&pbantelope.PermOp{
				Operation:   pbantelope.PermOp_OPERATION_UPDATE,
				ActionIndex: 0,
				OldPerm: &pbantelope.PermissionObject{
					Id:          4,
					ParentId:    2,
					Owner:       "eosio.old",
					Name:        "prod.major",
					LastUpdated: timestamppb.New(mustTimeParse("2018-06-08T08:08:08.888")),
					Authority:   auth,
				},
				NewPerm: &pbantelope.PermissionObject{
					Id:          4,
					ParentId:    3,
					Owner:       "eosio.new",
					Name:        "prod.major",
					LastUpdated: timestamppb.New(mustTimeParse("2018-06-08T08:08:08.888")),
					Authority:   auth,
				},
			},
			nil,
		},
		{
			`PERM_OP REM 0 3 {"parent":4,"owner":"eosio.rem","name":"prod.major","last_updated":"2018-06-08T08:08:08.888","auth":{"threshold":1,"keys":[],"accounts":[{"permission":{"actor":"eosio","permission":"active"},"weight":1}],"waits":[]}}`,
			&pbantelope.PermOp{
				Operation:   pbantelope.PermOp_OPERATION_REMOVE,
				ActionIndex: 0,
				OldPerm: &pbantelope.PermissionObject{
					Id:          3,
					ParentId:    4,
					Owner:       "eosio.rem",
					Name:        "prod.major",
					LastUpdated: timestamppb.New(mustTimeParse("2018-06-08T08:08:08.888")),
					Authority:   auth,
				},
				NewPerm: nil,
			},
			nil,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ctx := newParseCtx()
			err := ctx.readPermOp(test.line)

			require.Equal(t, test.expectedErr, err)

			if test.expectedErr == nil {
				require.Len(t, ctx.currentTrace.PermOps, 1)

				expected := protoJSONMarshalIndent(t, test.expected)
				actual := protoJSONMarshalIndent(t, ctx.currentTrace.PermOps[0])

				assert.JSONEq(t, expected, actual, diff.LineDiff(expected, actual))
			}
		})
	}
}

func Test_readABIDump_Start(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		expectedErr error
	}{
		{
			"version 12",
			`ABIDUMP START`,
			nil,
		},
		{
			"version 13",
			`ABIDUMP START 44 500`,
			nil,
		},
		{
			"version 13, invalid block num",
			`ABIDUMP START s44 500`,
			errors.New(`block_num is not a valid number, got: "s44"`),
		},
		{
			"version 13, invalid global sequence num",
			`ABIDUMP START 44 s500`,
			errors.New(`global_sequence_num is not a valid number, got: "s500"`),
		},
		{
			"invalid number of field",
			`ABIDUMP START 44`,
			errors.New(`expected to have either 2 or 4 fields, got 3`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newParseCtx()
			err := ctx.readABIStart(test.line)

			require.Equal(t, test.expectedErr, err)
		})
	}
}

func Test_readDeepMindVersion(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		software    string
		major       uint64
		minor       uint64
		expectedErr error
	}{
		{
			"version 13",
			`DEEP_MIND_VERSION leap 13 0`,
			"leap", 13, 0,
			nil,
		},
		{
			"version 13.1",
			`DEEP_MIND_VERSION leap 13 1`,
			"leap", 13, 1,
			nil,
		},
		{
			"version 13 old format",
			`DEEP_MIND_VERSION 13 1`,
			"", 0, 0,
			errors.New("invalid version format given \"DEEP_MIND_VERSION 13 1\", expected 'DEEP_MIND_VERSION ${software} ${major_version} ${minor_version}'"),
		},
		{
			"version 14, unsupported",
			`DEEP_MIND_VERSION leap 14 0`,
			"leap", 14, 0,
			errors.New("deep mind reported version 14, but this reader supports only 13"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newParseCtx()
			software, major, minor, _, err := ctx.readDeepmindVersion(test.line)

			require.Equal(t, test.software, software)
			require.Equal(t, test.major, major)
			require.Equal(t, test.minor, minor)
			require.Equal(t, test.expectedErr, err)
		})
	}
}

func Test_readABIDump_ABI(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		expectedErr error
	}{
		{
			"version 12",
			`ABIDUMP ABI 44 eosio AAAAAAAAAAAA`,
			nil,
		},
		{
			"version 13",
			`ABIDUMP ABI eosio AAAAAAAAAAAA`,
			nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newParseCtx()
			err := ctx.readABIDump(test.line)

			require.Equal(t, test.expectedErr, err)

			if test.expectedErr == nil {
				contractABI := ctx.abiDecoder.cache.findABI("eosio", 0)
				assert.NotNil(t, contractABI)
			}
		})
	}
}

func mustTimeParse(input string) time.Time {
	value, err := time.Parse("2006-01-02T15:04:05", input)
	if err != nil {
		panic(err)
	}

	return value
}

func reader(in string) io.Reader {
	return bytes.NewReader([]byte(in))
}

func protoJSONMarshalIndent(t *testing.T, message proto.Message) string {
	value, err := MarshalIndentToString(message, "  ")
	require.NoError(t, err)

	return value
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}

	if err != nil {
		return false
	}

	return !info.IsDir()
}

func blockWithConsole(block *pbantelope.Block) bool {
	for _, trxTrace := range block.TransactionTraces() {
		for _, actTrace := range trxTrace.ActionTraces {
			if len(actTrace.Console) > 0 {
				return true
			}
		}
	}

	return false
}

func newParseCtx() *parseCtx {
	return &parseCtx{
		hydrator:     antelope_v3_1.NewHydrator(zlogTest),
		abiDecoder:   newABIDecoder(false),
		currentBlock: &pbantelope.Block{},
		currentTrace: &pbantelope.TransactionTrace{},
	}
}

func isNil(v interface{}) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}
