package sessionhost

import "errors"

// Start every independently owned cleanup before joining any of them. A stuck
// dependency retains this join and its roots, but cannot delay the other jobs.
// Only return after every job actually completes; no timeout abandons ownership.
func joinOwnedCleanup(jobs ...func() error) error {
	results := make([]chan error, len(jobs))
	for i, job := range jobs {
		results[i] = make(chan error, 1)
		go func() { results[i] <- job() }()
	}
	var err error
	for _, result := range results {
		err = errors.Join(err, <-result)
	}
	return err
}
