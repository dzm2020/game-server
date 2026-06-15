package gateway

//const gatewayInternalErrCode = 1
//
//type clientAgent struct {
//	actor.BaseActor
//	component     *Component
//	conn          network.IConnection
//	session       internalroute.ISession
//	state         SessionState
//	asyncHandlers map[uint16]internalroute.AsyncHandler
//	syncHandlers  map[uint16]internalroute.SyncHandler
//}
//
//func newClientAgent(
//	component *Component,
//	conn network.IConnection,
//	asyncHandlers map[uint16]internalroute.AsyncHandler,
//	syncHandlers map[uint16]internalroute.SyncHandler,
//) *clientAgent {
//	r := &clientAgent{
//		component:     component,
//		conn:          conn,
//		asyncHandlers: asyncHandlers,
//		syncHandlers:  syncHandlers,
//	}
//	r.state = SessionState{
//		ConnID:      conn.ID(),
//		RemoteAddr:  conn.RemoteAddr(),
//		ConnectedAt: time.Now(),
//		LastActive:  time.Now(),
//		Meta:        make(map[string]string),
//		pending:     make(map[uint32]SessionCallback),
//	}
//	r.session = &baseSession{
//		Conn:  conn,
//		State: &r.state,
//	}
//	return r
//}
//
//func (r *clientAgent) OnMessage(ctx actor.Context) {
//	msg, ok := ctx.Message().(*protocol.Message)
//	if !ok {
//		return
//	}
//	r.handleProtocolMessage(ctx, msg)
//}
//
//func (r *clientAgent) handleProtocolMessage(ctx actor.Context, msg *protocol.Message) {
//	if msg == nil || msg.Head == nil || r.conn == nil {
//		return
//	}
//	r.state.LastActive = time.Now()
//
//	if msg.Index != 0 && r.invokePendingCallback(msg) {
//		return
//	}
//
//	if h, ok := r.asyncHandlers[msg.ID()]; ok {
//		h(r.session, msg)
//		return
//	}
//
//	if h, ok := r.syncHandlers[msg.ID()]; ok {
//		resp, err := h(r.session, msg)
//		if err != nil {
//			r.replyError(r.conn, msg, err)
//			return
//		}
//		if msg.Index == 0 || resp == nil {
//			return
//		}
//		r.replyMessage(r.conn, msg, resp)
//		return
//	}
//
//	targetActor := r.component.cfg.RouteActorName
//	if targetActor == "" {
//		r.replyError(r.conn, msg, errors.New("gateway route actor is empty"))
//		return
//	}
//
//	targetNodeID := r.component.cfg.RouteNodeID
//	if targetNodeID == "" {
//		targetNodeID = ctx.Self().NodeID
//	}
//
//	if targetNodeID == "" || targetNodeID == ctx.Self().NodeID {
//		r.handleLocalRoute(ctx, r.conn, msg, targetActor)
//		return
//	}
//	r.handleRemoteRoute(ctx, r.conn, msg, targetNodeID, targetActor)
//}
//
//func (r *clientAgent) handleLocalRoute(ctx actor.Context, conn network.IConnection, msg *protocol.Message, targetActorName string) {
//	if msg.Index == 0 {
//		if err := ctx.Tell(targetActorName, msg); err != nil {
//			r.replyError(conn, msg, err)
//		}
//		return
//	}
//
//	resp, err := ctx.System().Ask(ctx.Self(), targetActorName, msg, r.component.cfg.RequestTimeout)
//	if err != nil {
//		r.replyError(conn, msg, err)
//		return
//	}
//
//	data, ok := resp.([]byte)
//	if !ok {
//		r.replyError(conn, msg, fmt.Errorf("unexpected local route response type: %T", resp))
//		return
//	}
//	r.replyData(conn, msg, data)
//}
//
//func (r *clientAgent) handleRemoteRoute(ctx actor.Context, conn network.IConnection, msg *protocol.Message, targetNodeID string, targetActorName string) {
//	if r.component.cluster == nil {
//		r.replyError(conn, msg, errors.New("cluster is not ready"))
//		return
//	}
//
//	target := actor.NewPID(0, targetActorName, targetNodeID)
//	if msg.Index == 0 {
//		if err := r.component.cluster.SendToPID(ctx.Self(), target, msg); err != nil {
//			r.replyError(conn, msg, err)
//		}
//		return
//	}
//
//	data, err := r.component.cluster.RequestToPID(ctx.Self(), target, msg, r.component.cfg.RequestTimeout)
//	if err != nil {
//		r.replyError(conn, msg, err)
//		return
//	}
//	r.replyData(conn, msg, data)
//}
//
//func (r *clientAgent) replyData(conn network.IConnection, src *protocol.Message, data []byte) {
//	if conn == nil || src == nil || src.Head == nil {
//		return
//	}
//	reply := protocol.NewMessage(src.Cmd, src.Act, data)
//	reply.Copy(src)
//	_ = conn.Send(reply)
//}
//
//func (r *clientAgent) replyMessage(conn network.IConnection, src *protocol.Message, reply *protocol.Message) {
//	if conn == nil || src == nil || src.Head == nil || reply == nil {
//		return
//	}
//	if reply.Head == nil {
//		reply = protocol.NewWithData(reply.Data)
//	}
//	reply.Copy(src)
//	_ = conn.Send(reply)
//}
//
//func (r *clientAgent) replyError(conn network.IConnection, src *protocol.Message, err error) {
//	if conn == nil || src == nil || src.Head == nil {
//		return
//	}
//	reply := protocol.NewErr(src.Cmd, src.Act, gatewayInternalErrCode)
//	reply.Copy(src)
//	reply.Data = []byte(err.Error())
//	_ = conn.Send(reply)
//}
//
//type SessionCallback func(msg *protocol.Message)
//
//type baseSession struct {
//	Conn  network.IConnection
//	State *SessionState
//}
//
//type SessionState struct {
//	UserID      string
//	ConnID      uint64
//	RemoteAddr  string
//	ConnectedAt time.Time
//	LastActive  time.Time
//	Meta        map[string]string
//
//	mu      sync.Mutex
//	pending map[uint32]SessionCallback
//}
//
//func (r *clientAgent) registerPendingCallback(index uint32, cb SessionCallback) {
//	if index == 0 || cb == nil {
//		return
//	}
//	r.state.mu.Lock()
//	r.state.pending[index] = cb
//	r.state.mu.Unlock()
//}
//
//func (r *clientAgent) invokePendingCallback(msg *protocol.Message) bool {
//	if msg == nil || msg.Head == nil || msg.Index == 0 {
//		return false
//	}
//	r.state.mu.Lock()
//	cb, ok := r.state.pending[msg.Index]
//	if ok {
//		delete(r.state.pending, msg.Index)
//	}
//	r.state.mu.Unlock()
//	if ok && cb != nil {
//		cb(msg)
//		return true
//	}
//	return false
//}
//
//func (r *clientAgent) OnDestroy(actor.Context) {
//	r.state.mu.Lock()
//	r.state.pending = make(map[uint32]SessionCallback)
//	r.state.mu.Unlock()
//}
