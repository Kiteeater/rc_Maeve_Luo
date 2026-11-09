package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"notifier/internal/enums"
)

// WriteID 是稳定动作元组的 SHA-256 十六进制。
// 元组包含任务 ID、动作，以及该动作的尝试、操作、Outbox 和发布代次。
// 长度前缀编码，标识内部的分隔符不会把两个不同元组收成同一个键。
// 事务重试必须复用同一元组，因此不会追加第二条历史。
func WriteID(deliveryID uint64, action enums.HistoryPhase, attemptID uint64, operationID string, outboxID uint64, publishGeneration uint32) (string, error) {
	if !knownPhase(action) {
		return "", fmt.Errorf("domain: unknown history action")
	}
	buf := make([]byte, 0, 64+len(operationID))
	buf = binary.BigEndian.AppendUint64(buf, deliveryID)
	buf = appendString(buf, string(action))
	buf = binary.BigEndian.AppendUint64(buf, attemptID)
	buf = appendString(buf, operationID)
	buf = binary.BigEndian.AppendUint64(buf, outboxID)
	buf = binary.BigEndian.AppendUint32(buf, publishGeneration)
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), nil
}

func appendString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(s)))
	return append(dst, s...)
}

func knownPhase(action enums.HistoryPhase) bool {
	for _, phase := range enums.HistoryPhases {
		if phase == action {
			return true
		}
	}
	return false
}
