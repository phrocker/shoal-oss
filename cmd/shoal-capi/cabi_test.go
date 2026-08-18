package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSharedLibraryCABI(t *testing.T) {
	if goEnv(t, "CGO_ENABLED") != "1" {
		t.Skip("C ABI requires CGO_ENABLED=1")
	}
	cc := compilerCommand(t, "CC")
	cxx := compilerCommand(t, "CXX")
	root := repositoryRoot(t)
	artifacts, err := os.MkdirTemp(root, ".shoal-cabi-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(artifacts)

	libraryName := "libshoal.so"
	executableName := "lifecycle"
	if runtime.GOOS == "windows" {
		libraryName = "shoal.dll"
		executableName += ".exe"
	} else if runtime.GOOS == "darwin" {
		libraryName = "libshoal.dylib"
	}
	library := filepath.Join(artifacts, libraryName)
	runCommand(
		t,
		root,
		nil,
		"go",
		"build",
		"-tags=shoal_capi_test",
		"-buildmode=c-shared",
		"-o",
		library,
		"./cmd/shoal-capi",
	)

	include := filepath.Join(root, "capi", "include")
	cppObject := filepath.Join(artifacts, "header_cpp_test.o")
	cppArgs := append(
		append([]string{}, cxx.args...),
		"-std=c++11", "-Wall", "-Wextra", "-Werror",
		"-I", include,
		"-c", filepath.Join(root, "capi", "tests", "header_cpp_test.cpp"),
		"-o", cppObject,
	)
	runCommand(t, root, nil, cxx.name, cppArgs...)

	executable := filepath.Join(artifacts, executableName)
	cArgs := append(
		append([]string{}, cc.args...),
		"-std=c11", "-Wall", "-Wextra", "-Werror",
		"-I", include,
		"-I", filepath.Join(root, "capi", "tests"),
		filepath.Join(root, "capi", "tests", "lifecycle.c"),
		library,
		"-o", executable,
	)
	runCommand(t, root, nil, cc.name, cArgs...)

	env := append([]string{}, os.Environ()...)
	switch runtime.GOOS {
	case "windows":
		env = prependEnvPath(env, "PATH", artifacts)
	case "darwin":
		env = prependEnvPath(env, "DYLD_LIBRARY_PATH", artifacts)
	default:
		env = prependEnvPath(env, "LD_LIBRARY_PATH", artifacts)
	}
	runCommand(t, artifacts, env, executable)

	bridgeExecutable := filepath.Join(artifacts, "result_bridge")
	if runtime.GOOS == "windows" {
		bridgeExecutable += ".exe"
	}
	bridgeArgs := append(
		append([]string{}, cc.args...),
		"-std=c11", "-Wall", "-Wextra", "-Werror",
		"-I", include,
		"-I", filepath.Join(root, "cmd", "shoal-capi"),
		filepath.Join(root, "capi", "tests", "result_bridge.c"),
		filepath.Join(root, "cmd", "shoal-capi", "bridge.c"),
		"-o", bridgeExecutable,
	)
	runCommand(t, root, nil, cc.name, bridgeArgs...)
	runCommand(t, artifacts, nil, bridgeExecutable)
}

type command struct {
	name string
	args []string
}

func compilerCommand(t *testing.T, variable string) command {
	t.Helper()
	fields := strings.Fields(goEnv(t, variable))
	if len(fields) == 0 {
		t.Fatalf("go env %s returned an empty command", variable)
	}
	path, err := exec.LookPath(fields[0])
	if err != nil {
		t.Skipf("%s compiler %q is unavailable: %v", variable, fields[0], err)
	}
	return command{name: path, args: fields[1:]}
}

func goEnv(t *testing.T, name string) string {
	t.Helper()
	output, err := exec.Command("go", "env", name).CombinedOutput()
	if err != nil {
		t.Fatalf("go env %s: %v\n%s", name, err, output)
	}
	return strings.TrimSpace(string(output))
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func runCommand(t *testing.T, directory string, env []string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = directory
	if env != nil {
		cmd.Env = env
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, output)
	}
}

func prependEnvPath(env []string, name, value string) []string {
	prefix := name + "="
	for index, entry := range env {
		if strings.HasPrefix(strings.ToUpper(entry), strings.ToUpper(prefix)) {
			env[index] = fmt.Sprintf("%s=%s%c%s", name, value, os.PathListSeparator, entry[len(prefix):])
			return env
		}
	}
	return append(env, name+"="+value)
}
