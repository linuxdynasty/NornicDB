package fabric

import "context"

type subTransactionContextKey struct{}
type fabricTransactionContextKey struct{}

// WithSubTransaction returns a context carrying the active fabric sub-transaction.
func WithSubTransaction(ctx context.Context, sub *SubTransaction) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, subTransactionContextKey{}, sub)
}

// SubTransactionFromContext returns the active fabric sub-transaction from context.
func SubTransactionFromContext(ctx context.Context) (*SubTransaction, bool) {
	if ctx == nil {
		return nil, false
	}
	sub, ok := ctx.Value(subTransactionContextKey{}).(*SubTransaction)
	return sub, ok && sub != nil
}

// WithFabricTransaction returns a context carrying the active Fabric transaction.
func WithFabricTransaction(ctx context.Context, tx *FabricTransaction) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, fabricTransactionContextKey{}, tx)
}

// FabricTransactionFromContext returns the active Fabric transaction from context.
func FabricTransactionFromContext(ctx context.Context) (*FabricTransaction, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(fabricTransactionContextKey{}).(*FabricTransaction)
	return tx, ok && tx != nil
}
