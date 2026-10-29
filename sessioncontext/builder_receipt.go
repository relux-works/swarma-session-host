package sessioncontext

import sm "github.com/relux-works/swarma-session-host/sessionmanager"

// These aliases preserve the original protocol8 wire shapes. The concrete
// host gateway owns trace production/closure; hashes alone grant no authority.
type ContextRepositoryAccessAttestation = sm.ContextRepositoryAccessAttestation
type BuilderGatewayFinalization = sm.BuilderGatewayFinalization
