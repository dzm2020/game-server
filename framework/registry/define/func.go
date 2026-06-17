package define

type Topology struct {
	All     []ServiceInstance
	Added   []ServiceInstance
	Updated []ServiceInstance
	Removed []ServiceInstance
}

type ServiceChangeHandler func(topology *Topology)

// ServiceInstance represents a discovered service endpoint.
type ServiceInstance struct {
	ID      string
	Name    string
	Address string
	Port    int
	Tags    []string
	Meta    map[string]string
}

type HealthState = string

const (
	HealthStatePassing  = HealthState("passing")
	HealthStateWarning  = HealthState("warning")
	HealthStateCritical = HealthState("critical")
)
