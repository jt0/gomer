package logs

import (
	"log"
	"os"
)

var Error = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Llongfile)
