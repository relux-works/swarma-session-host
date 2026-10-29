package sessionmanager

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func readProviderProcessIdentity(pid int) (providerProcessIdentity, error) {
	root := "/proc/" + strconv.Itoa(pid)
	info, err := os.Stat(root)
	if err != nil {
		return providerProcessIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return providerProcessIdentity{}, fmt.Errorf("unreadable provider process uid")
	}
	data, err := os.ReadFile(root + "/stat")
	if err != nil {
		return providerProcessIdentity{}, err
	}
	// comm is parenthesized and may itself contain spaces or ')'. The fields
	// after its final ')' begin with field 3; starttime is field 22 (clock ticks).
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return providerProcessIdentity{}, fmt.Errorf("unreadable provider process stat")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return providerProcessIdentity{}, fmt.Errorf("short provider process stat")
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return providerProcessIdentity{}, err
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return providerProcessIdentity{}, fmt.Errorf("unreadable provider process birth time")
	}
	// starttime is relative to boot. Bind it to this boot as well so an old
	// snapshot cannot match a new process after a machine restart.
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(boot)) == "" {
		return providerProcessIdentity{}, fmt.Errorf("unreadable provider process boot identity")
	}
	return providerProcessIdentity{PID: pid, UID: stat.Uid, Start: strings.TrimSpace(string(boot)) + ":" + fields[19], PGID: pgid}, nil
}
