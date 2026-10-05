# consistenthash

并发安全的 Go 一致性哈希环（虚拟节点），仅依赖标准库，无第三方服务。

## 特性

- 每个真实节点映射为固定数量的虚拟节点（创建时指定 `replicas`，必须大于 0）
- 使用标准库 FNV-1a 64 位哈希（`hash/fnv`），位置稳定、跨运行可复现
- 有序位置切片在 Add/Remove 时增量维护，`Get` 只做一次二分查找，不重建、不重排整个环
- 同一哈希位置可挂多个虚拟节点，位置冲突不会静默丢失真实节点或虚拟节点
- `sync.RWMutex` 保证并发安全；一次 Add/Remove 对其他 goroutine 表现为完整的原子操作

## API

```go
ring, err := consistenthash.New(100) // 每个真实节点 100 个虚拟节点
err = ring.Add("node-a")             // 重复添加返回 ErrNodeAlreadyExists，空名返回 ErrEmptyNodeName
err = ring.Remove("node-a")          // 删除不存在节点返回 ErrNodeNotFound
node, err := ring.Get("user-1")      // 空环返回 ErrEmptyRing；否则顺时针命中、环尾回绕
members := ring.Members()            // 稳定字典序，返回切片与内部状态隔离
n := ring.Len()                      // 真实节点数量
r := ring.Replicas()                 // 每个真实节点的虚拟节点数量
```

## 运行演示

```sh
go run ./cmd/demo
```

演示使用内置节点 `node-a/node-b/node-c` 和固定 key，展示 Add、Get、
Remove 后的重新路由以及 Members 输出。

## 运行测试

```sh
go test ./...
go test -race ./...
```

测试覆盖 Add/Remove、重复与空节点名、固定 key 路由、环尾回绕、虚拟节点
数量、Members 稳定顺序与隔离性、删除后重新路由、哈希位置冲突，以及一个
基础并发场景（用 `-race` 验证并发安全）。
