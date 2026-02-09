package interop

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"testing"

	"github.com/cjanietz/automerge-native-go/automerge"
	"github.com/cjanietz/automerge-native-go/internal/model"
	"github.com/cjanietz/automerge-native-go/internal/storage"
	intsync "github.com/cjanietz/automerge-native-go/internal/sync"
)

type suiteResult struct {
	Name    string         `json:"name"`
	Passed  int            `json:"passed"`
	Failed  int            `json:"failed"`
	Skipped int            `json:"skipped"`
	Cases   []caseResult   `json:"cases"`
	Meta    map[string]any `json:"meta,omitempty"`
}

type caseResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type compatibilityReport struct {
	Version string        `json:"version"`
	Suites  []suiteResult `json:"suites"`
}

func TestCompatibilityHarness(t *testing.T) {
	report := compatibilityReport{Version: "phase10-v1"}
	report.Suites = append(report.Suites, runRustFixturesSuite())
	report.Suites = append(report.Suites, runFuzzCrashersSuite())
	report.Suites = append(report.Suites, runExemplarSuite())
	report.Suites = append(report.Suites, runRoundtripSuite())
	report.Suites = append(report.Suites, runSyncInteropSuite())

	if path := os.Getenv("COMPAT_REPORT_PATH"); path != "" {
		if err := writeReport(path, report); err != nil {
			t.Fatalf("write compatibility report: %v", err)
		}
	}

	for _, s := range report.Suites {
		if s.Failed > 0 {
			t.Fatalf("compatibility suite %q failed: passed=%d failed=%d skipped=%d", s.Name, s.Passed, s.Failed, s.Skipped)
		}
	}
}

func runRustFixturesSuite() suiteResult {
	suite := suiteResult{Name: "rust-fixtures"}
	files, ok := collectFiles(filepath.Join("..", "..", "..", "rust", "automerge", "tests", "fixtures"))
	if !ok {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "fixtures-dir", Status: "skipped", Detail: "directory not found"})
		return suite
	}
	for _, f := range files {
		if !strings.HasSuffix(f, ".automerge") {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: filepath.Base(f), Status: "failed", Detail: err.Error()})
			continue
		}
		if _, err := automerge.LoadWithOptions(data, automerge.LoadOptions{OnPartialLoad: automerge.OnPartialIgnore, Verification: automerge.VerificationCheck}); err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: filepath.Base(f), Status: "failed", Detail: err.Error()})
			continue
		}
		suite.Passed++
		suite.Cases = append(suite.Cases, caseResult{Name: filepath.Base(f), Status: "passed"})
	}
	if suite.Passed+suite.Failed == 0 {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "fixtures-files", Status: "skipped", Detail: "no .automerge files found"})
	}
	return suite
}

func runFuzzCrashersSuite() suiteResult {
	suite := suiteResult{Name: "fuzz-crashers", Meta: map[string]any{"assertion": "must not panic"}}
	files, ok := collectFiles(filepath.Join("..", "..", "..", "rust", "automerge", "tests", "fuzz-crashers"))
	if !ok {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "crashers-dir", Status: "skipped", Detail: "directory not found"})
		return suite
	}
	for _, f := range files {
		base := filepath.Base(f)
		status, detail := runCrashingLoadCase(f)
		suite.Cases = append(suite.Cases, caseResult{Name: base, Status: status, Detail: detail})
		switch status {
		case "passed":
			suite.Passed++
		case "skipped":
			suite.Skipped++
		default:
			suite.Failed++
		}
	}
	if suite.Passed+suite.Failed+suite.Skipped == 0 {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "crashers-files", Status: "skipped", Detail: "no files found"})
	}
	return suite
}

func runExemplarSuite() suiteResult {
	suite := suiteResult{Name: "interop-exemplar"}
	dir := filepath.Join("..", "..", "..", "interop", "exemplar")
	files, ok := collectFiles(dir)
	if !ok {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "exemplar-dir", Status: "skipped", Detail: "directory not found"})
		return suite
	}
	for _, f := range files {
		base := filepath.Base(f)
		if !strings.HasSuffix(f, ".automerge") && base != "exemplar" {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: base, Status: "failed", Detail: err.Error()})
			continue
		}
		doc, err := automerge.LoadWithOptions(data, automerge.LoadOptions{OnPartialLoad: automerge.OnPartialIgnore, Verification: automerge.VerificationCheck})
		if err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: base, Status: "failed", Detail: err.Error()})
			continue
		}
		saved, err := doc.Save()
		if err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: base, Status: "failed", Detail: fmt.Sprintf("save: %v", err)})
			continue
		}
		if len(saved) == 0 {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: base, Status: "failed", Detail: "empty save output"})
			continue
		}
		suite.Passed++
		suite.Cases = append(suite.Cases, caseResult{Name: base, Status: "passed"})
	}
	if suite.Passed+suite.Failed == 0 {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "exemplar-files", Status: "skipped", Detail: "no .automerge files found"})
	}
	return suite
}

func runRoundtripSuite() suiteResult {
	suite := suiteResult{Name: "cross-impl-roundtrip", Meta: map[string]any{"expectation": "Rust save -> Go load -> Go save -> Rust parser load"}}
	files, ok := collectFiles(filepath.Join("..", "..", "..", "rust", "automerge", "tests", "fixtures"))
	if !ok {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "fixtures-dir", Status: "skipped", Detail: "directory not found"})
		return suite
	}
	for _, f := range files {
		if !strings.HasSuffix(f, ".automerge") {
			continue
		}
		in, err := os.ReadFile(f)
		if err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: filepath.Base(f), Status: "failed", Detail: err.Error()})
			continue
		}
		doc, err := automerge.LoadWithOptions(in, automerge.LoadOptions{OnPartialLoad: automerge.OnPartialIgnore, Verification: automerge.VerificationCheck})
		if err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: filepath.Base(f), Status: "failed", Detail: fmt.Sprintf("go load: %v", err)})
			continue
		}
		out, err := doc.Save()
		if err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: filepath.Base(f), Status: "failed", Detail: fmt.Sprintf("go save: %v", err)})
			continue
		}
		if _, err := storage.ParseRustChunks(out); err != nil {
			suite.Failed++
			suite.Cases = append(suite.Cases, caseResult{Name: filepath.Base(f), Status: "failed", Detail: fmt.Sprintf("rust parser load: %v", err)})
			continue
		}
		suite.Passed++
		suite.Cases = append(suite.Cases, caseResult{Name: filepath.Base(f), Status: "passed"})
	}
	if suite.Passed+suite.Failed == 0 {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "roundtrip-files", Status: "skipped", Detail: "no .automerge files found"})
	}
	return suite
}

func runSyncInteropSuite() suiteResult {
	suite := suiteResult{Name: "sync-interop"}
	fixture := filepath.Join("..", "..", "..", "rust", "automerge", "tests", "fixtures", "two_change_chunks.automerge")
	data, err := os.ReadFile(fixture)
	if err != nil {
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "go-rust-fixture", Status: "skipped", Detail: err.Error()})
		suite.Skipped++
		suite.Cases = append(suite.Cases, caseResult{Name: "go-js-peer", Status: "skipped", Detail: "js fixture not configured"})
		return suite
	}

	rustPeer, err := automerge.LoadWithOptions(data, automerge.DefaultLoadOptions())
	if err != nil {
		suite.Failed++
		suite.Cases = append(suite.Cases, caseResult{Name: "go-rust-fixture", Status: "failed", Detail: err.Error()})
	} else {
		goPeer := automerge.NewDocument()
		sRustToGo := intsync.NewState()
		sGoToRust := intsync.NewState()
		caps := []intsync.Capability{intsync.CapabilityMessageV1, intsync.CapabilityMessageV2}
		sRustToGo.TheirCapabilities = &caps
		sGoToRust.TheirCapabilities = &caps
		emptyHeads := []model.ChangeHash{}
		emptyNeed := []model.ChangeHash{}
		emptyHave := []intsync.Have{}
		sRustToGo.TheirHeads = &emptyHeads
		sRustToGo.TheirNeed = &emptyNeed
		sRustToGo.TheirHave = &emptyHave

		msg, genErr := rustPeer.Sync().GenerateSyncMessage(sRustToGo)
		if genErr != nil || msg == nil {
			suite.Failed++
			detail := "nil message"
			if genErr != nil {
				detail = genErr.Error()
			}
			suite.Cases = append(suite.Cases, caseResult{Name: "go-rust-fixture", Status: "failed", Detail: detail})
		} else {
			enc, encErr := msg.Encode()
			if encErr != nil {
				suite.Failed++
				suite.Cases = append(suite.Cases, caseResult{Name: "go-rust-fixture", Status: "failed", Detail: encErr.Error()})
			} else {
				dec, decErr := intsync.DecodeMessage(enc)
				if decErr != nil {
					suite.Failed++
					suite.Cases = append(suite.Cases, caseResult{Name: "go-rust-fixture", Status: "failed", Detail: decErr.Error()})
				} else if recvErr := goPeer.Sync().ReceiveSyncMessage(sGoToRust, dec); recvErr != nil {
					suite.Failed++
					suite.Cases = append(suite.Cases, caseResult{Name: "go-rust-fixture", Status: "failed", Detail: recvErr.Error()})
				} else {
					suite.Passed++
					suite.Cases = append(suite.Cases, caseResult{Name: "go-rust-fixture", Status: "passed"})
				}
			}
		}
	}

	suite.Skipped++
	suite.Cases = append(suite.Cases, caseResult{Name: "go-js-peer", Status: "skipped", Detail: "js fixture not configured"})
	return suite
}

func runCrashingLoadCase(path string) (status, detail string) {
	if fi, err := os.Stat(path); err != nil {
		return "failed", err.Error()
	} else if fi.IsDir() {
		return "skipped", "directory"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "failed", err.Error()
	}
	defer func() {
		if r := recover(); r != nil {
			status = "failed"
			detail = fmt.Sprintf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	_, _ = automerge.LoadWithOptions(data, automerge.LoadOptions{OnPartialLoad: automerge.OnPartialIgnore, Verification: automerge.VerificationCheck})
	return "passed", ""
}

func collectFiles(dir string) ([]string, bool) {
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, false
	}
	if !fi.IsDir() {
		return []string{dir}, true
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out)
	return out, true
}

func writeReport(path string, report compatibilityReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(buf, '\n'), 0o644)
}
