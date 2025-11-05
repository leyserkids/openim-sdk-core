package conversation_msg

import (
	"context"
	"sort"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
)

func (c *Conversation) projectGroupReadInfo(ctx context.Context, conversationID string, clientMsgIDs []string) error {

	if len(clientMsgIDs) == 0 {
		return nil
	}

	cursorState, err := c.db.GetGroupReadCursorState(ctx, conversationID)
	if err != nil {
		return errs.WrapMsg(err, "GetGroupReadCursorState failed", "conversationID", conversationID)
	}
	var cursorVersion int64
	if cursorState != nil {
		cursorVersion = cursorState.CursorVersion
	}

	var messages []*model_struct.LocalChatLog
	for _, clientMsgID := range clientMsgIDs {
		message, err := c.waitForMessageSyncSeq(ctx, conversationID, clientMsgID)
		if err != nil {
			log.ZWarn(ctx, "waitForMessageSyncSeq failed", err, "conversationID", conversationID, "clientMsgID", clientMsgID)
			continue
		}
		messages = append(messages, message)
	}
	if len(messages) == 0 {
		return nil
	}

	cursors, err := c.db.GetGroupReadCursorsByConversationID(ctx, conversationID) // []{UserID, MaxReadSeq}
	if err != nil {
		return errs.WrapMsg(err, "GetGroupReadCursorsByConversationID failed", "conversationID", conversationID)
	}
	memberCount := len(cursors)

	sortedMaxSeqs := make([]int64, 0, memberCount)
	type pair struct {
		uid string
		seq int64
	}
	userSeqPairs := make([]pair, 0, memberCount)

	for _, cur := range cursors {
		sortedMaxSeqs = append(sortedMaxSeqs, cur.MaxReadSeq)
		userSeqPairs = append(userSeqPairs, pair{uid: cur.UserID, seq: cur.MaxReadSeq})
	}
	sort.Slice(sortedMaxSeqs, func(i, j int) bool { return sortedMaxSeqs[i] < sortedMaxSeqs[j] })

	for _, m := range messages {
		if m == nil || m.Seq == 0 {
			continue
		}
		var attach sdk_struct.AttachedInfoElem
		utils.JsonStringToStruct(m.AttachedInfo, &attach)

		alreadyFresh := (attach.GroupHasReadInfo.ReadCursorVersion == cursorVersion)
		if alreadyFresh {
			continue
		}

		// Calculate read count excluding the sender
		list := make([]string, 0, memberCount)
		for _, p := range userSeqPairs {
			if p.seq >= m.Seq && p.uid != m.SendID {
				list = append(list, p.uid)
			}
		}
		hasReadCount := len(list)

		attach.GroupHasReadInfo.HasReadCount = int32(hasReadCount)
		attach.GroupHasReadInfo.GroupMemberCount = int32(memberCount)
		attach.GroupHasReadInfo.ReadCursorVersion = cursorVersion
		attach.GroupHasReadInfo.HasReadUserIDList = list

		m.AttachedInfo = utils.StructToJsonString(attach)
		if err := c.db.UpdateMessage(ctx, conversationID, m); err != nil {
			log.ZWarn(ctx, "projectGroupReadInfo UpdateMessage err", err, "conversationID", conversationID, "seq", m.Seq, "clientMsgID", m.ClientMsgID)
		}
	}

	return nil
}
