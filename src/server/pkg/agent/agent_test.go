package agent

import (
	"strings"
	"testing"
)

func TestSetEnvReplace(t *testing.T) {
	env := []string{"A=1", "B=2"}
	env = setEnv(env, "A", "9")
	if len(env) != 2 {
		t.Fatalf("setEnv appended instead of replacing: %v", env)
	}
	found := false
	for _, e := range env {
		if e == "A=9" {
			found = true
		}
		if strings.HasPrefix(e, "A=") && e != "A=9" {
			t.Fatalf("old A value left behind: %v", env)
		}
	}
	if !found {
		t.Fatalf("A=9 not present: %v", env)
	}
}

func TestSetEnvAppend(t *testing.T) {
	env := []string{"A=1"}
	env = setEnv(env, "NEW", "v")
	if len(env) != 2 || env[1] != "NEW=v" {
		t.Fatalf("append failed: %v", env)
	}
}
