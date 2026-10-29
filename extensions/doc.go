// Package extensions provides opt-in sink factories and the file-logger bootstrap for loginjector.
// Every factory returns a mutex-guarded io.Writer.
//
// Print handlers include PrintHandler for plain stdout lines,
// TimestampedPrintHandler for timestamped stdout lines, and TimestampedHandler for
// adding timestamps to another writer. WithTimeLayout configures the timestamp
// layout, and WithOutput selects the stdout handler's destination.
package extensions
