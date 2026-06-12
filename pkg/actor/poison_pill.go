package actor

type poisonPill struct{}

var PoisonPill any = poisonPill{}

func isPoisonPill(v any) bool {
	_, ok := v.(poisonPill)
	return ok
}
