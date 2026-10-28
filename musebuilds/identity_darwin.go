package musebuilds

import "golang.org/x/sys/unix"

// O_SEARCH = O_EXEC|O_DIRECTORY in Darwin's public sys/fcntl.h. x/sys
// v0.43.0 omits O_EXEC/O_SEARCH. This is search access, not read access;
// it preserves execute-only path traversal without admitting enumeration.
const directorySearchFlags = 0x40000000 | unix.O_DIRECTORY

func statTimes(st unix.Stat_t) (int64, int64, int64, int64) {
	return st.Mtim.Sec, st.Mtim.Nsec, st.Ctim.Sec, st.Ctim.Nsec
}
