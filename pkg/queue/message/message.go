package message

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ValerySidorin/charon/pkg/wal"
	"github.com/pkg/errors"
)

type Message struct {
	Version int
	Type    string
}

func NewMessage(raw string) (*Message, error) {
	tokens := strings.Split(raw, "_")
	if len(tokens) != 2 {
		return nil, errors.New("invalid message raw input (len)")
	}

	version, err := strconv.Atoi(tokens[0])
	if err != nil {
		return nil, errors.Wrap(err, "invalid message raw input (version)")
	}

	if tokens[1] != wal.FileTypeDiff && tokens[1] != wal.FileTypeFull {
		return nil, errors.New("invalid message raw input (type)")
	}

	return &Message{
		Version: version,
		Type:    tokens[1],
	}, nil
}

func (m *Message) String() string {
	return fmt.Sprintf("%d_%s", m.Version, m.Type)
}
