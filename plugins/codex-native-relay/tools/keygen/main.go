// Command keygen 生成 Ed25519 发布者密钥对：<out>.private（仅保存在受控环境）与 <out>.public。
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	out := flag.String("out", "build/keys/publisher", "输出路径前缀")
	flag.Parse()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err == nil {
		err = os.MkdirAll(filepath.Dir(*out), 0o700)
	}
	if err == nil {
		err = os.WriteFile(*out+".private", []byte(base64.StdEncoding.EncodeToString(private)+"\n"), 0o600)
	}
	if err == nil {
		err = os.WriteFile(*out+".public", []byte(base64.StdEncoding.EncodeToString(public)+"\n"), 0o644)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "keygen:", err)
		os.Exit(1)
	}
	fmt.Printf("公钥（配置到 plugins.trusted_publishers）: %s\n", base64.StdEncoding.EncodeToString(public))
}
