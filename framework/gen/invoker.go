package gen

type ILocalInvoker interface {
	Handler(from *PID, target *PID, msg *Message) error
}
