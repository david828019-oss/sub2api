// Command packager 生成带文件哈希的 manifest.json，并打包（可选签名）为 .s2plugin。
package main

import (
	"archive/zip"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

type runtimeFlags map[string]string

func (r runtimeFlags) String() string { return fmt.Sprint(map[string]string(r)) }

func (r runtimeFlags) Set(value string) error {
	key, file, ok := strings.Cut(value, "=")
	if !ok || key == "" || file == "" {
		return errors.New("runtime 格式应为 <goos>-<goarch>=<binary path>")
	}
	r[key] = file
	return nil
}

func main() {
	runtimes := runtimeFlags{}
	source := flag.String("source", "manifest.source.json", "清单源文件")
	uiDir := flag.String("ui", "ui", "UI 目录")
	version := flag.String("version", "", "覆盖清单版本")
	output := flag.String("output", "", "输出 .s2plugin 路径")
	signingKey := flag.String("signing-key", "", "Base64 Ed25519 私钥文件（keygen 生成）")
	keyID := flag.String("key-id", "", "签名 key_id，必须与部署方 trusted_publishers 的键一致")
	flag.Var(runtimes, "runtime", "<goos>-<goarch>=<binary path>，可重复")
	flag.Parse()

	if err := run(*source, *uiDir, *version, *output, *signingKey, *keyID, runtimes); err != nil {
		fmt.Fprintln(os.Stderr, "packager:", err)
		os.Exit(1)
	}
}

type entry struct {
	archivePath string
	diskPath    string
	executable  bool
}

func run(source, uiDir, version, output, signingKey, keyID string, runtimes runtimeFlags) error {
	if output == "" || len(runtimes) == 0 {
		return errors.New("必须提供 -output 和至少一个 -runtime")
	}
	if (signingKey == "") != (keyID == "") {
		return errors.New("-signing-key 与 -key-id 必须同时提供")
	}
	raw, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	var manifest map[string]any
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return fmt.Errorf("解析清单源文件: %w", err)
	}
	if version != "" {
		manifest["version"] = strings.TrimPrefix(version, "v")
	}

	var entries []entry
	runtimeManifest := map[string]any{}
	for key, binary := range runtimes {
		name := "plugin"
		if strings.HasPrefix(key, "windows-") {
			name = "plugin.exe"
		}
		archivePath := path.Join("runtimes", key, name)
		runtimeManifest[key] = map[string]string{"path": archivePath}
		entries = append(entries, entry{archivePath: archivePath, diskPath: binary, executable: true})
	}
	err = filepath.WalkDir(uiDir, func(diskPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("UI 目录只能包含普通文件: %s", diskPath)
		}
		relative, err := filepath.Rel(uiDir, diskPath)
		if err != nil {
			return err
		}
		entries = append(entries, entry{archivePath: path.Join("ui", filepath.ToSlash(relative)), diskPath: diskPath})
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].archivePath < entries[j].archivePath })

	files := map[string]string{}
	for _, item := range entries {
		content, err := os.ReadFile(item.diskPath)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(content)
		files[item.archivePath] = hex.EncodeToString(sum[:])
	}
	manifest["runtimes"] = runtimeManifest
	manifest["files"] = files
	manifestRaw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	var signatureRaw []byte
	if signingKey != "" {
		signatureRaw, err = sign(manifestRaw, signingKey, keyID)
		if err != nil {
			return err
		}
	}
	return writeArchive(output, manifestRaw, signatureRaw, entries)
}

func sign(manifestRaw []byte, keyFile, keyID string) ([]byte, error) {
	encoded, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil || len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("签名私钥必须是 Base64 编码的 64 字节 Ed25519 私钥")
	}
	signature := ed25519.Sign(ed25519.PrivateKey(key), manifestRaw)
	return json.MarshalIndent(map[string]string{
		"algorithm": "ed25519",
		"key_id":    keyID,
		"signature": base64.StdEncoding.EncodeToString(signature),
	}, "", "  ")
}

func writeArchive(output string, manifestRaw, signatureRaw []byte, entries []entry) error {
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return err
	}
	file, err := os.Create(output)
	if err != nil {
		return err
	}
	archive := zip.NewWriter(file)
	add := func(name string, content []byte, mode fs.FileMode) error {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.SetMode(mode)
		writer, err := archive.CreateHeader(header)
		if err != nil {
			return err
		}
		_, err = writer.Write(content)
		return err
	}
	err = add("manifest.json", manifestRaw, 0o644)
	if err == nil && signatureRaw != nil {
		err = add("signature.json", signatureRaw, 0o644)
	}
	for _, item := range entries {
		if err != nil {
			break
		}
		var content []byte
		content, err = os.ReadFile(item.diskPath)
		if err != nil {
			break
		}
		mode := fs.FileMode(0o644)
		if item.executable {
			mode = 0o755
		}
		err = add(item.archivePath, content, mode)
	}
	if closeErr := archive.Close(); err == nil {
		err = closeErr
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}
