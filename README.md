# consistenthash

并发安全的一致性哈希环。纯 Go 标准库实现，无第三方依赖。

## 特性

- 每个真实节点映射为固定数量的虚拟节点（`replicas > 0`），位置由 crc32（IEEE）生成
- 环结构随 `Add`/`Remove` 增量维护并保持有序；`Get` 只做一次二分查找，不会重建或重排整个环
- 同一哈希位置可容纳多个虚拟节点，冲突不会导致真实节点或虚拟节点丢失
- `sync.RWMutex` 保护全部操作；一次 `Add`/`Remove` 对其他 goroutine 原子可见

## API

| 方法 | 说明 |
| --- | --- |
| `New(replicas int) (*Ring, error)` | 创建哈希环；`replicas <= 0` 返回错误 |
| `Add(node string) bool` | 添加真实节点；名称为空或已存在时返回 `false` |
| `Remove(node string) bool` | 删除真实节点及其全部虚拟节点；节点不存在时返回 `false` |
| `Get(key string) (string, bool)` | 顺时针路由 key（环尾回绕）；空环返回 `("", false)` |
| `Members() []string` | 真实节点名称的字典序列表（副本，与内部状态隔离） |
| `Len() int` | 当前真实节点数量 |
| `Replicas() int` | 每个真实节点的虚拟节点数量 |

## 使用示例

\`\`\`go
ring, err := consistenthash.New(100)
if err != nil {
	log.Fatal(err)
}
ring.Add("node-a")
ring.Add("node-b")

if node, ok := ring.Get("some-key"); ok {
	fmt.Println("routed to", node)
}
\`\`\`

## 运行演示

\`\`\`sh
go run ./cmd/demo
\`\`\`

## 运行测试

\`\`\`sh
go test -race ./...
\`\`\`
