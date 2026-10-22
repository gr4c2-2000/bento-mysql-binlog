// Command bento-mysql is a Bento distribution with the mysql_binlog input
// compiled in. Everything else behaves exactly like upstream Bento.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/warpstreamlabs/bento/public/service"

	// Pull in every standard Bento component.
	_ "github.com/warpstreamlabs/bento/public/components/all"

	"github.com/gr4c2-2000/bento-mysql-binlog/internal/binlog"
)

func main() {
	if err := binlog.Register(); err != nil {
		fmt.Fprintf(os.Stderr, "registering mysql_binlog input: %v\n", err)
		os.Exit(1)
	}
	service.RunCLI(context.Background())
}
