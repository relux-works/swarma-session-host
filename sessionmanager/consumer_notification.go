package sessionmanager

import "encoding/json"

type CodexConsumerNotification struct {
	Method string
	Params json.RawMessage
}
