# Code Style

We have not created strong opinions for code style and formatting, but instead deferred to tooling defaults where sensible.

We prefer consistency of style over any individuals preference.

## Linting and formatting tooling

[golangci-lint](https://golangci-lint.run/) is used for linting and formatting.
 
Linting run with `golangci-lint run` and formatting with `golangci-lint fmt`.

IDEs have support as described in the [docs](https://golangci-lint.run/welcome/integrations/)

It can be installed using go with the command
`go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`

### Visual Studio Code

There is an extension for `golangci-lint`, however the functionality we've found useful so far is all built in.

We configure the relevant settings in `.vscode/settings.json`.

### GoLand

GoLand has built in support for golangci-lint.