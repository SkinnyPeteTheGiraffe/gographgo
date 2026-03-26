package graph

import "testing"

func TestInterruptTasks_RequiresUpdatesAndMatchingNode(t *testing.T) {
	tasks := []pregelTask[map[string]any]{
		{id: "t1", name: "alpha"},
		{id: "t2", name: "beta"},
	}

	updatedVersions := map[string]int64{"messages": 2}
	seenBefore := map[string]map[string]int64{pregelInterrupt: {"messages": 1}}

	matches := interruptTasks(tasks, []string{"beta"}, updatedVersions, seenBefore)
	if len(matches) != 1 || matches[0].name != "beta" {
		t.Fatalf("interruptTasks matches = %#v, want [beta]", matches)
	}

	noNameMatch := interruptTasks(tasks, []string{"gamma"}, updatedVersions, seenBefore)
	if len(noNameMatch) != 0 {
		t.Fatalf("interruptTasks no-name-match = %#v, want empty", noNameMatch)
	}

	noUpdates := interruptTasks(tasks, []string{"beta"}, updatedVersions, map[string]map[string]int64{pregelInterrupt: {"messages": 2}})
	if len(noUpdates) != 0 {
		t.Fatalf("interruptTasks no-updates = %#v, want empty", noUpdates)
	}
}

func TestInterruptResults_RequiresUpdatesAndMatchingNode(t *testing.T) {
	results := []pregelTaskResult{
		{taskID: "r1", node: "alpha"},
		{taskID: "r2", node: "beta"},
	}

	updatedVersions := map[string]int64{"messages": 3}
	seenBefore := map[string]map[string]int64{pregelInterrupt: {"messages": 1}}

	matches := interruptResults(results, []string{"beta"}, updatedVersions, seenBefore)
	if len(matches) != 1 || matches[0].node != "beta" {
		t.Fatalf("interruptResults matches = %#v, want [beta]", matches)
	}

	allMatches := interruptResults(results, []string{All}, updatedVersions, seenBefore)
	if len(allMatches) != len(results) {
		t.Fatalf("interruptResults all-matches len = %d, want %d", len(allMatches), len(results))
	}

	noNameMatch := interruptResults(results, []string{"gamma"}, updatedVersions, seenBefore)
	if len(noNameMatch) != 0 {
		t.Fatalf("interruptResults no-name-match = %#v, want empty", noNameMatch)
	}

	noUpdates := interruptResults(results, []string{"beta"}, updatedVersions, map[string]map[string]int64{pregelInterrupt: {"messages": 3}})
	if len(noUpdates) != 0 {
		t.Fatalf("interruptResults no-updates = %#v, want empty", noUpdates)
	}
}
