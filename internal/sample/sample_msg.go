package sample

import (
	"encoding/json"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
)

type KafkaSampleMsg struct {
	Id        string `json:"id"`
	FirstName string `json:"f_name"`
	LastName  string `json:"l_name"`
	Timestamp string `json:"ts"`
	Text      string `json:"text"`
}

func (msg *KafkaSampleMsg) MarshallJson() ([]byte, error) {
	return json.Marshal(msg)
}

func NewRndKakfaSampleMsg() KafkaSampleMsg {
	now := time.Now()
	return KafkaSampleMsg{
		Id:        uuid.New().String(),
		FirstName: gofakeit.FirstName(),
		LastName:  gofakeit.LastName(),
		Timestamp: now.Format(time.RFC3339),
		Text:      gofakeit.SentenceSimple(),
	}
}

func NewRndKakfaSampleMsgBatch(size int) []KafkaSampleMsg {
	batch := make([]KafkaSampleMsg, size)
	for i := 0; i < size; i++ {
		batch[i] = NewRndKakfaSampleMsg()
	}

	return batch
}
