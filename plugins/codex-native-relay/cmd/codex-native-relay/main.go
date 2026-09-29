// Command codex-native-relay 是 Sub2API 插件进程入口，只负责启动 gRPC 服务。
package main

import (
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/plugin"
)

func main() {
	pluginv1.Serve(plugin.NewServer())
}
