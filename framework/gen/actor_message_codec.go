package gen

import (
	"encoding/binary"
)

func Encode(msg *Message) []byte {
	msg.Len = uint32(len(msg.Data))
	buf := make([]byte, HeadLen+len(msg.Data))
	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], msg.Len)
	offset += 4
	buf[offset] = msg.Cmd
	offset += 1
	buf[offset] = msg.Act
	offset += 1
	binary.BigEndian.PutUint16(buf[offset:], msg.Error)
	offset += 2
	binary.BigEndian.PutUint32(buf[offset:], msg.Index)
	offset += 4
	copy(buf[offset:], msg.Data)
	return buf
}

func Decode(buf []byte) (*Message, int) {

	if len(buf) < HeadLen {
		return nil, 0
	}
	l := binary.BigEndian.Uint32(buf)
	total := HeadLen + int(l)
	if len(buf) < total {
		return nil, 0
	}

	msg := &Message{Head: &Head{}}
	offset := 0
	msg.Len = binary.BigEndian.Uint32(buf[offset : offset+4])
	offset += 4
	msg.Cmd = buf[offset]
	offset += 1
	msg.Act = buf[offset]
	offset += 1
	msg.Error = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	msg.Index = binary.BigEndian.Uint32(buf[offset : offset+4])
	offset += 4
	msg.Data = make([]byte, msg.Len)
	copy(msg.Data, buf[offset:total])

	return msg, total
}
