// Package httptap provides opt-in HTTP middleware for loginjector. It captures
// request and response payloads with NewPayloadHandler and recovers panics with
// NewRecoverHandler. For one access-log line per request, see
// extensions.NewAccessHandler.
//
// Consumers that do not need HTTP middleware can omit this package.
package httptap
