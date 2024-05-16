package mr

import (
	"fmt"
	"os"
	"time"
)

const Debug = false

var file *os.File

func Dprintf(format string, val ...interface{}) {
	now := time.Now()
	info := fmt.Sprintf("%v-%v-%v %v:%v:%v:  ", now.Year(), int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second()) + fmt.Sprintf(format+"\n", val...)

	if Debug {
		fmt.Println(info)
	} else {
		file.WriteString(info)
	}
}
