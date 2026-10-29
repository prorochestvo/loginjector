// Package loginjector provides a fan-out Logger and error types for Go programs.
// A Logger sends each message to configured io.Writer sinks and level hooks.
//
// Optional sinks, the file-logger bootstrap, and the Axiom sink are in
// github.com/prorochestvo/loginjector/extensions. The level ladder and closer
// helpers are in github.com/prorochestvo/loginjector/adapters. HTTP payload and
// panic-recovery middleware are in github.com/prorochestvo/loginjector/httptap.
package loginjector
