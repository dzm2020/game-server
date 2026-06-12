package cluster

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"actor"
	"game-server/internal/protocol"

	"google.golang.org/protobuf/proto"
)

const NodeSubjectPrefix = "cluster.node."

var (
	ErrNilProtocolMessage = errors.New("protocol message is nil")
	ErrEmptyNodeID        = errors.New("target node id is required")
	ErrEmptyActorID       = errors.New("target actor id is required")
	ErrInvalidProtoData   = errors.New("invalid protobuf data")
)

func newClusterMessage(sourcePID actor.PID, targetPID actor.PID, msg *protocol.Message) (*ClusterMessage, error) {
	target := actorPIDToProto(targetPID)
	source := actorPIDToProto(sourcePID)
	if err := ValidateTargetPID(target); err != nil {
		return nil, err
	}
	if err := ValidateTargetPID(source); err != nil {
		return nil, err
	}
	if msg == nil || msg.Head == nil {
		return nil, ErrNilProtocolMessage
	}

	return &ClusterMessage{
		CreatedAt: time.Now().UnixMilli(),
		TargetPid: target,
		SourcePid: source,
		Message: &ActorMessage{
			Cmd:   uint32(msg.Cmd),
			Act:   uint32(msg.Act),
			Error: uint32(msg.Error),
			Index: msg.Index,
			Data:  msg.Data,
		},
	}, nil
}

func (m *ClusterMessage) ToProtocolMessage() *protocol.Message {
	if m == nil || m.Message == nil {
		return nil
	}
	return &protocol.Message{
		Head: &protocol.Head{
			Cmd:   uint8(m.Message.Cmd),
			Act:   uint8(m.Message.Act),
			Error: uint16(m.Message.Error),
			Index: m.Message.Index,
		},
		Data: m.Message.Data,
	}
}

func (m *ClusterMessage) Marshal() ([]byte, error) {
	if m == nil {
		return nil, errors.New("cluster message is nil")
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return proto.Marshal(m)
}

func UnmarshalClusterMessage(data []byte) (*ClusterMessage, error) {
	msg := new(ClusterMessage)
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidProtoData, err)
	}
	return msg, nil
}

func (m *ClusterMessage) Validate() error {
	if m == nil {
		return errors.New("cluster message is nil")
	}
	if m.TargetPid == nil {
		return ErrEmptyNodeID
	}
	if err := ValidateTargetPID(m.TargetPid); err != nil {
		return err
	}
	return nil
}

func ValidateTargetPID(pid *ActorPID) error {
	if pid == nil {
		return ErrEmptyNodeID
	}
	if strings.TrimSpace(pid.NodeId) == "" {
		return ErrEmptyNodeID
	}
	if pid.ActorId == 0 && strings.TrimSpace(pid.ActorName) == "" {
		return ErrEmptyActorID
	}
	return nil
}

func NodeSubject(nodeID string) (string, error) {
	if strings.TrimSpace(nodeID) == "" {
		return "", ErrEmptyNodeID
	}
	return fmt.Sprintf("%s%s", NodeSubjectPrefix, nodeID), nil
}

func actorPIDToProto(pid actor.PID) *ActorPID {
	return &ActorPID{
		ActorId:   pid.ActorID,
		ActorName: pid.ActorName,
		NodeId:    pid.NodeID,
	}
}
func protoPIDToActor(pid *ActorPID) actor.PID {
	if pid == nil {
		return actor.NoSender
	}
	return actor.PID{
		ActorID:   pid.ActorId,
		ActorName: pid.ActorName,
		NodeID:    pid.NodeId,
	}
}

func NewRespondErrClusterMessage(request *ClusterMessage, handleErr error) *ClusterMessage {
	reply := &ClusterMessage{
		TraceId:   request.GetTraceId(),
		CreatedAt: time.Now().UnixMilli(),
		TargetPid: request.GetSourcePid(),
		SourcePid: request.GetTargetPid(),
		Message: &ActorMessage{
			Error: clusterInternalErrorCode,
			Data:  []byte(handleErr.Error()),
		},
	}
	return reply
}
func NewRespondDataClusterMessage(request *ClusterMessage, data []byte) *ClusterMessage {
	reply := &ClusterMessage{
		TraceId:   request.GetTraceId(),
		CreatedAt: time.Now().UnixMilli(),
		TargetPid: request.GetSourcePid(),
		SourcePid: request.GetTargetPid(),
		Message: &ActorMessage{
			Data: data,
		},
	}
	return reply
}
