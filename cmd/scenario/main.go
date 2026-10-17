// Command scenario runs one tuning demo against the Postgres pointed to by the
// libpq environment variables. Each scenario prints what it measured before
// and after its fix; the output is meant to be read, not parsed.
//
//	scenario <name> [flags]
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strings"
	"time"
)

type scenario struct {
	about string
	run   func(ctx context.Context, args []string) error
}

var scenarios = map[string]scenario{}

func register(name, about string, run func(ctx context.Context, args []string) error) {
	scenarios[name] = scenario{about, run}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	s, ok := scenarios[os.Args[1]]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	start := time.Now()
	fmt.Printf("# scenario %s (%s)\n", os.Args[1], start.UTC().Format(time.RFC3339))
	if err := s.run(ctx, os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "scenario %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
	fmt.Printf("\n# done in %s\n", time.Since(start).Round(time.Second))
}

func usage() {
	names := make([]string, 0, len(scenarios))
	for n := range scenarios {
		names = append(names, n)
	}
	slices.Sort(names)
	var b strings.Builder
	b.WriteString("usage: scenario <name> [flags]\n\n")
	for _, n := range names {
		fmt.Fprintf(&b, "  %-18s %s\n", n, scenarios[n].about)
	}
	fmt.Fprint(os.Stderr, b.String())
}
