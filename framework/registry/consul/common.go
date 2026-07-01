package consul

import (
	"game-server/framework/pkg/netutil"
	"net"
	"strconv"
	"strings"

	"github.com/hashicorp/consul/api"
)

// entriesToInstances
//
//	@Description: 将 Consul ServiceEntry 列表转换为统一的 ServiceInstance 列表
//	@param entries
//	@return []ServiceInstance
func entriesToInstances(entries []*api.ServiceEntry) map[string]ServiceInstance {
	instances := make(map[string]ServiceInstance, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.Service == nil {
			continue
		}

		extAddress := ""
		if entry.Service.Meta != nil {
			if v := entry.Service.Meta["ext_address"]; v != "" {
				extAddress = v
			}

		}
		instances[entry.Service.ID] = ServiceInstance{
			ID:         entry.Service.ID,
			Name:       entry.Service.Service,
			ExtAddress: extAddress,
			RpcAddress: net.JoinHostPort(entry.Service.Address, strconv.Itoa(entry.Service.Port)),
			Tags:       entry.Service.Tags,
			Meta:       entry.Service.Meta,
		}
	}
	return instances
}

// instanceToRegistration
//
//	@Description: 将instance转换成consul api需要的格式
//	@receiver rr
//	@param reg
//	@param options
//	@return *api.AgentServiceRegistration
func instanceToRegistration(reg ServiceInstance, options Options) (*api.AgentServiceRegistration, error) {
	host, rawPort, err := netutil.SplitHostPort(reg.RpcAddress)
	if err != nil {
		return nil, err
	}
	checkID := serviceCheckID(reg.ID)
	check := &api.AgentServiceCheck{
		CheckID:                        checkID,
		TTL:                            options.TTL.String(),
		DeregisterCriticalServiceAfter: options.DeregisterAfter.String(),
	}
	meta := make(map[string]string, len(reg.Meta)+2)
	for k, v := range reg.Meta {
		meta[k] = v
	}
	meta["ext_address"] = reg.ExtAddress
	serviceReg := &api.AgentServiceRegistration{
		ID:      reg.ID,
		Name:    reg.Name,
		Address: host,
		Port:    rawPort,
		Tags:    reg.Tags,
		Meta:    meta,
		Check:   check,
	}
	return serviceReg, nil
}

func serviceCheckID(serviceID string) string {
	if serviceID == "" {
		return ""
	}
	if strings.HasPrefix(serviceID, "service:") {
		return serviceID
	}
	return "service:" + serviceID
}

func cloneServiceInstances(items []ServiceInstance) []ServiceInstance {
	if len(items) == 0 {
		return []ServiceInstance{}
	}
	out := make([]ServiceInstance, len(items))
	copy(out, items)
	return out
}
