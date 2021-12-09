package models_test

import (
	"os"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/redisx/assertredis"
	"github.com/stretchr/testify/require"
)

func TestRecordFlowStatistics(t *testing.T) {
	ctx, rt, _, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetRedis)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	assetsJSON, err := os.ReadFile("testdata/flow_stats_test.json")
	require.NoError(t, err)

	session1, session1Sprint1 := test.NewSessionBuilder().WithAssets(assetsJSON).WithFlow("19eab6aa-4a88-42a1-8882-b9956823c680").MustBuild()
	session2, session2Sprint1 := test.NewSessionBuilder().WithAssets(assetsJSON).WithFlow("19eab6aa-4a88-42a1-8882-b9956823c680").MustBuild()
	session3, session3Sprint1 := test.NewSessionBuilder().WithAssets(assetsJSON).WithFlow("19eab6aa-4a88-42a1-8882-b9956823c680").MustBuild()

	err = models.RecordFlowStatistics(ctx, rt, nil, []flows.Session{session1, session2}, []flows.Sprint{session1Sprint1, session2Sprint1, session3Sprint1})
	require.NoError(t, err)

	// no contacts have left exits on a node type that we record operands for yet
	assertredis.Keys(t, rp, []string{})

	_, session1Sprint2, err := test.ResumeSession(session1, assetsJSON, "blue")
	require.NoError(t, err)
	_, session2Sprint2, err := test.ResumeSession(session2, assetsJSON, "BLUE")
	require.NoError(t, err)
	session3, session3Sprint2, err := test.ResumeSession(session3, assetsJSON, "teal")
	require.NoError(t, err)
	_, session3Sprint3, err := test.ResumeSession(session3, assetsJSON, "azure")
	require.NoError(t, err)

	err = models.RecordFlowStatistics(ctx, rt, nil, []flows.Session{session1, session2}, []flows.Sprint{session1Sprint2, session2Sprint2, session3Sprint2, session3Sprint3})
	require.NoError(t, err)

	assertredis.Keys(t, rp, []string{
		"recent_operands:c02fc3ba-369a-4c87-9bc4-c3b376bda6d2:57b50d33-2b5a-4726-82de-9848c61eff6e", // color split :: Blue exit -> next node
		"recent_operands:ea6c38dc-11e2-4616-9f3e-577e44765d44:8712db6b-25ff-4789-892c-581f24eeeb95", // color split :: Other exit -> next node
		"recent_operands:2b698218-87e5-4ab8-922e-e65f91d12c10:88d8bf00-51ce-4e5e-aae8-4f957a0761a0", // split by expression :: Other exit -> next node
	})

	// check recent operands for color split :: Blue exit -> next node
	assertredis.ZRange(t, rp, "recent_operands:c02fc3ba-369a-4c87-9bc4-c3b376bda6d2:57b50d33-2b5a-4726-82de-9848c61eff6e", 0, -1,
		[]string{"ab235a56-26f1-44ed-a41f-4712a5656d9a|blue", "bebe0625-d242-4dae-a6ad-d91f03ef0186|BLUE"},
	)

	// check recent operands for color split :: Other exit -> next node
	assertredis.ZRange(t, rp, "recent_operands:ea6c38dc-11e2-4616-9f3e-577e44765d44:8712db6b-25ff-4789-892c-581f24eeeb95", 0, -1,
		[]string{"1201a412-d775-43cd-ba77-5ab563883e6e|teal", "bc05e2fa-3d7e-4d94-bcd3-9957520f471c|azure"},
	)

	// check recent operands for split by expression :: Other exit -> next node
	assertredis.ZRange(t, rp, "recent_operands:2b698218-87e5-4ab8-922e-e65f91d12c10:88d8bf00-51ce-4e5e-aae8-4f957a0761a0", 0, -1,
		[]string{"e4482ad3-280f-42ad-8854-dc254e6d2221|0", "7126c123-7b29-40e2-a677-cdd483eaa0b7|0"},
	)
}
