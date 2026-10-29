package sessionmanager

// These exact typed request constructors are shared with the original host
// input tests; they contain no transport implementation or test verdicts.
func codexHostSubmission(lease ControllerLease, id, command, text string) InputSubmission {
	return InputSubmission{Lease: lease, ID: id, Payload: RenderedInput{CodexTurn: &CodexTurnCommand{CommandID: command, Start: &CodexTurnStart{Text: text}}}, Expected: AckExpectation{Kind: "provider-ack", Correlation: command}}
}
func textInput(lease ControllerLease, id, text string) InputSubmission {
	return InputSubmission{Lease: lease, ID: id, Payload: RenderedInput{Text: &text}, Expected: AckExpectation{Kind: "written"}}
}
