package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Role 是可停止的后台角色。Run 必须在 ctx 取消后有界返回。具体业务循环由 S03～S06 完成。
type Role interface {
	Name() string
	Run(ctx context.Context) error
}

// Group 同时运行一组角色。任一角色返回非取消错误时，取消其余角色并返回该错误。
type Group struct {
	roles []Role
}

// NewGroup 构造角色组。S00 没有业务角色，空组只等待取消。
func NewGroup(roles ...Role) *Group {
	return &Group{roles: append([]Role(nil), roles...)}
}

// Run 阻塞到 ctx 取消且全部角色退出，或某个角色失败。
func (g *Group) Run(ctx context.Context) error {
	if g == nil || len(g.roles) == 0 {
		<-ctx.Done()
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(g.roles))
	for _, role := range g.roles {
		wg.Add(1)
		go func(role Role) {
			defer wg.Done()
			err := role.Run(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("server: role %s: %w", role.Name(), err)
				cancel()
			}
		}(role)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errCh:
		<-done
		return err
	case <-ctx.Done():
		<-done
		return nil
	case <-done:
		return nil
	}
}
