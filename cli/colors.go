package cli

import (
	"os"

	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
)

func SupportsColor(noColorHint bool) {
	fd := os.Stdout.Fd()
	color.NoColor = noColorHint || (!isatty.IsTerminal(fd) && !isatty.IsCygwinTerminal(fd))
}
