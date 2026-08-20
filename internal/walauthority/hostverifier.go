package walauthority

import (
	"context"
	"fmt"

	"github.com/phrocker/shoal/internal/ingestrouter"
	"github.com/phrocker/shoal/internal/tserver"
)

// HostedOwnerVerifier binds the WAL authority to the exact Host assignment
// attempt and ServiceLock generations that brought the tablet online.
type HostedOwnerVerifier struct {
	Host    *tserver.Host
	Fence   tserver.Fence
	Attempt tserver.Attempt
}

func (v HostedOwnerVerifier) Verify(ctx context.Context, fence ingestrouter.Fence) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v.Host == nil || !v.Attempt.Valid() {
		return ErrStaleOwner
	}
	if fence.ServerGeneration != v.Fence.Server.String() ||
		fence.ManagerGeneration != v.Fence.Manager.String() ||
		fence.Assignment != v.Attempt.Assignment() {
		return fmt.Errorf("%w: request fence does not name hosted attempt", ErrStaleOwner)
	}
	return v.Host.VerifyHosted(v.Fence, v.Attempt)
}
