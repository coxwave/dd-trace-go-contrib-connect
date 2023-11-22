package connect

const (
	tagMethodName = "connect.method.name"
	tagMethodKind = "connect.method.kind"
	tagCode       = "connect.code"
)

const (
	methodKindUnary        = "unary"
	methodKindClientStream = "client_streaming"
	methodKindServerStream = "server_streaming"
	methodKindBidiStream   = "bidi_streaming"
)

const (
	extRPCSystemConnect = "connect"
)
