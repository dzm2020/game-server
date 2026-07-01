package component

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
)

var (
	ErrComponentCannotBeNil                = fmt.Errorf("组件不能为空")
	ErrComponentTypeCannotBeNil            = fmt.Errorf("组件类型不能为空")
	ErrCannotRegisterComponentAfterStarted = fmt.Errorf("组件启动后无法注册组件")
	ErrCannotRemoveComponentAfterStarted   = fmt.Errorf("组件启动后无法移除组件")
	ErrComponentAlreadyRegistered          = fmt.Errorf("组件已注册")
	ErrComponentNotRegistered              = fmt.Errorf("组件未注册")
	ErrManagerAlreadyStarted               = fmt.Errorf("管理器已启动")
	ErrManagerStoppedCannotRestart         = fmt.Errorf("管理器已停止，无法重启")
	ErrFailedToStartComponent              = fmt.Errorf("组件启动失败")
)

type IManager interface {
	Init(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	ComponentCount() int
	GetComponent(t any) IComponent
	AddComponent(component ...IComponent) error
	RemoveComponent(t IComponent) error
	Range(fn func(component IComponent))
}

type Manager struct {
	components   *maputil.ConcurrentMap[reflect.Type, IComponent]
	order        []reflect.Type // 保存组件注册顺序
	orderMu      sync.RWMutex
	started      atomic.Bool
	stopped      atomic.Bool
	stopOnce     sync.Once
	firstStopErr error // 首次 Stop 时的错误，后续 Stop 调用仍返回该错误
}

// NewComponentsMgr 创建新的生命周期管理器
func NewComponentsMgr() *Manager {
	return &Manager{
		components: maputil.NewConcurrentMap[reflect.Type, IComponent](10),
		order:      make([]reflect.Type, 0),
	}
}

// IsStarted 检查管理器是否已启动
func (cm *Manager) IsStarted() bool {
	return cm.started.Load()
}

// IsStopped 检查管理器是否已停止
func (cm *Manager) IsStopped() bool {
	return cm.stopped.Load()
}

// ComponentCount 返回已注册的组件数量
func (cm *Manager) ComponentCount() int {
	cm.orderMu.RLock()
	defer cm.orderMu.RUnlock()
	return len(cm.order)
}

func (cm *Manager) GetComponent(t any) IComponent {
	componentType := reflect.TypeOf(t)
	if componentType == nil {
		return nil
	}
	component, _ := cm.components.Get(componentType)
	return component
}

// AddComponent 注册组件，按注册顺序启动，按逆序停止。
func (cm *Manager) AddComponent(component ...IComponent) error {
	if cm.started.Load() {
		return ErrCannotRegisterComponentAfterStarted
	}

	cm.orderMu.Lock()
	defer cm.orderMu.Unlock()

	for _, c := range component {
		componentType := reflect.TypeOf(component)
		// 检查是否已注册同类型组件
		if _, exists := cm.components.Get(componentType); exists {
			return ErrComponentAlreadyRegistered
		}

		// 注册组件
		cm.components.Set(componentType, c)
		cm.order = append(cm.order, componentType)
	}
	return nil
}

// RemoveComponent 根据组件类型移除已注册组件。
// 仅允许在管理器启动前移除，避免破坏运行中的生命周期顺序。
func (cm *Manager) RemoveComponent(t IComponent) error {
	if cm.started.Load() {
		return ErrCannotRemoveComponentAfterStarted
	}
	if t == nil {
		return ErrComponentCannotBeNil
	}
	componentType := reflect.TypeOf(t)
	if componentType == nil {
		return ErrComponentTypeCannotBeNil
	}

	cm.orderMu.Lock()
	defer cm.orderMu.Unlock()

	if _, exists := cm.components.Get(componentType); !exists {
		return ErrComponentNotRegistered
	}

	cm.components.Delete(componentType)
	for idx, typ := range cm.order {
		if typ == componentType {
			cm.order = append(cm.order[:idx], cm.order[idx+1:]...)
			break
		}
	}
	return nil
}

func (cm *Manager) Init(ctx context.Context) error {
	if cm.started.Load() {
		return ErrManagerAlreadyStarted
	}

	cm.orderMu.RLock()
	order := make([]reflect.Type, len(cm.order))
	copy(order, cm.order)
	cm.orderMu.RUnlock()

	for _, key := range order {
		component, exists := cm.components.Get(key)
		if !exists {
			continue
		}

		if err := component.Init(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Start 初始化并启动所有已注册的组件
func (cm *Manager) Start(ctx context.Context) (err error) {
	if cm.stopped.Load() {
		return ErrManagerStoppedCannotRestart
	}
	if cm.started.Load() {
		return ErrManagerAlreadyStarted
	}

	cm.orderMu.RLock()
	order := make([]reflect.Type, len(cm.order))
	copy(order, cm.order)
	cm.orderMu.RUnlock()

	var started []IComponent
	for _, key := range order {
		component, exists := cm.components.Get(key)
		if !exists {
			continue
		}

		if err = component.Start(ctx); err != nil {
			_ = cm.stopComponents(ctx, started)
			return fmt.Errorf("%w: type=%s err=%v", ErrFailedToStartComponent, key.String(), err)
		}

		started = append(started, component)
	}

	cm.started.Store(true)

	return nil
}

// Stop 停止所有已注册的组件
// 按注册顺序的逆序依次停止，确保依赖关系正确；首次调用返回停止过程中的错误，后续调用仍返回该错误。
func (cm *Manager) Stop(ctx context.Context) error {
	cm.stopOnce.Do(func() {
		if !cm.started.Load() {
			return
		}
		if cm.stopped.Load() {
			return
		}
		cm.stopped.Store(true)

		cm.orderMu.RLock()
		order := make([]reflect.Type, len(cm.order))
		copy(order, cm.order)
		cm.orderMu.RUnlock()

		// 按逆序获取组件
		components := make([]IComponent, 0, len(order))
		for i := len(order) - 1; i >= 0; i-- {
			if component, exists := cm.components.Get(order[i]); exists {
				components = append(components, component)
			}
		}

		cm.firstStopErr = cm.stopComponents(ctx, components)
	})
	return cm.firstStopErr
}

func (cm *Manager) Range(fn func(component IComponent)) {
	cm.components.Range(func(key reflect.Type, value IComponent) bool {
		fn(value)
		return true
	})
}

// stopComponents 停止组件列表（组件列表应该已经是逆序的）
func (cm *Manager) stopComponents(ctx context.Context, components []IComponent) error {
	var lastErr error
	for _, component := range components {
		if component == nil {
			continue
		}
		if err := component.Stop(ctx); err != nil {
			lastErr = err
			continue
		}
	}
	return lastErr
}

// StopWithTimeout 使用超时停止所有组件
func (cm *Manager) StopWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return cm.Stop(ctx)
}
