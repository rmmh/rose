package server

import "github.com/rmmh/rose/storage"

// quarantineRelocationLocked retains both physical candidates but removes every
// route to the affected mounted vlog. Relocation admission excludes active I/O;
// vlogMu excludes new I/O while its clients are closed. Other vlogs cannot share
// this plog because CapturePlogRelocation requires exclusive ownership.
// Quiescent Recover reopens the authoritative catalog placement and clears the
// fence only after all vlogs mount. No retry may copy over either candidate first.
func (s *Server) quarantineRelocationLocked(vlogID, plogID uint32, old, replacement *storage.Plog, cause error) {
	if s.quarantinedVlogs == nil {
		s.quarantinedVlogs = make(map[uint32]error)
	}
	s.quarantinedVlogs[vlogID] = cause
	s.clearActiveVlogLocked(vlogID)
	delete(s.vlogs, vlogID)
	delete(s.plogs, plogID)
	delete(s.offlinePlogs, plogID)
	if old != nil {
		_ = old.Close()
	}
	if replacement != nil && replacement != old {
		_ = replacement.Close()
	}
}
