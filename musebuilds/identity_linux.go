package musebuilds

import "golang.org/x/sys/unix"

const directorySearchFlags = unix.O_PATH

func statTimes(st unix.Stat_t) (int64, int64, int64, int64) {
	return st.Mtim.Sec, st.Mtim.Nsec, st.Ctim.Sec, st.Ctim.Nsec
}
