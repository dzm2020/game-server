package cluster

import (
	"fmt"

	"actor"
)

type nodeMessageSubscriber struct {
	cluster *Cluster
}

const clusterInternalErrorCode uint32 = 1
const defaultSourceActorName = "__cluster__"

func (s *nodeMessageSubscriber) OnMessage(request []byte, isSync bool, response func(data []byte) error) {
	err := s.handleIncomingClusterMessage(request, response)
	if err != nil {
		if isSync && response != nil {
			s.respondSyncError(request, err, response)
		}
	}
}

func (s *nodeMessageSubscriber) respondSyncError(request []byte, handleErr error, response func(data []byte) error) {
	msg, err := UnmarshalClusterMessage(request)
	if err != nil || msg == nil {
		_ = response([]byte(handleErr.Error()))
		return
	}
	reply := NewRespondErrClusterMessage(msg, handleErr)
	payload, marshalErr := reply.Marshal()
	if marshalErr != nil {
		_ = response([]byte(handleErr.Error()))
		return
	}
	_ = response(payload)
}

func (s *nodeMessageSubscriber) shouldIgnoreForOtherNode(localNodeID string, targetPID *ActorPID) bool {
	if targetPID == nil {
		return false
	}
	return localNodeID != "" && targetPID.GetNodeId() != localNodeID
}

func (s *nodeMessageSubscriber) decodeIncomingMessage(data []byte) (*ClusterMessage, string, actor.ISystem, error) {
	msg, err := UnmarshalClusterMessage(data)
	if err != nil {
		return nil, "", nil, err
	}
	if err = msg.Validate(); err != nil {
		return nil, "", nil, err
	}
	if msg.GetTargetPid() == nil {
		return nil, "", nil, ErrEmptyActorID
	}
	localNodeID := s.cluster.currentNodeID()
	system := s.cluster.system
	if system == nil {
		return nil, "", nil, ErrClusterNoActorSystem
	}
	return msg, localNodeID, system, nil
}

func (s *nodeMessageSubscriber) handleIncomingClusterMessage(data []byte, response func(data []byte) error) error {
	msg, localNodeID, system, err := s.decodeIncomingMessage(data)
	if err != nil {
		return err
	}
	if s.shouldIgnoreForOtherNode(localNodeID, msg.GetTargetPid()) {
		return nil
	}
	localMsg := msg.ToProtocolMessage()
	if localMsg == nil {
		return ErrNilProtocolMessage
	}

	return system.SendEnvelope(protoPIDToActor(msg.GetTargetPid()), actor.Envelope{
		Payload: localMsg,
		Sender:  protoPIDToActor(msg.SourcePid),
		Respond: s.clusterResponder(msg, response),
	})
}

func (s *nodeMessageSubscriber) clusterResponder(msg *ClusterMessage, response func(data []byte) error) actor.Responder {
	if response == nil {
		return nil
	}
	return func(v any) error {
		bin, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("%w: %T", ErrInvalidActorResponse, v)
		}
		replyMessage := NewRespondDataClusterMessage(msg, bin)
		payload, marshalErr := replyMessage.Marshal()
		if marshalErr != nil {
			return marshalErr
		}
		return response(payload)
	}
}
