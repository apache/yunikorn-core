# Coding Guidelines

## The basics
GO as a language provides a build in formatter for the code: `gofmt`.
The project uses the predefined format as implemented in `gofmt`.
That means tabs and not spaces etc.
Read the [Effective GO](https://golang.org/doc/effective_go.html) page for further details.
Before creating a pull request make sure the code at least is formatted using `gofmt`.

Beside the effective GO guide follow the recommendations from the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments) wiki page.
The wiki provides a good collection of comments from code reviews.
Most of the comments will be checked in the automated checks described below.

When using an IDE, like GoLand or Visual Studio Code, use the builtin options.
Most IDEs will provide an extensive list of checks or formatting options that help formatting and point out code issues. 

## Automated checks
Not all code will be written using an IDE.
Even between contributors the settings might not be the same in all installs.
To help keep code formatted consistently a [lint](https://en.wikipedia.org/wiki/Lint_(software) tool is part of the code approval.   

There are a large number of lint tools are available for Go.
Most of the lint tools only check one specific thing.
Some of the tools will aggregate a number of linters and provide an overview of all the issues found. 
For the project we have chosen the [golangci-lint](https://github.com/golangci/golangci-lint) tool.
The tool can be run locally and will be integrated into the GitHub PR flow.

### Installing and running locally
Depending on your development system the install might differ slightly.
Follow the [install instructions](https://github.com/golangci/golangci-lint#install) provided by the project.

After the tool is installed you can run it using the standard command line: 
```shell script
golangci-lint run
```
The projects still generate a number of warnings.
The high impact warnings have been fixed or ignored via comments, see [False positives](#false-positives).

If you have been working on a new feature or a bug you only want to check the files that have changed.
You can run the tool with the option `--new` or `--new-from-rev` option.
The `--new` option will only check uncommitted files.
The `--new-from-rev` option will check changes against a specific committed revision.

```shell script
# for uncommitted changes
golangci-lint run --new
# for committed changes against a revision
golangci-lint run --new-from-rev=origin/master
```
The `make` integration checks two locations for the executable and uses the `--new` command line option.
* `go env GOPATH)/bin/`
* `./bin/` (relative to the project)

The make target is `lint`:
```shell script
make lint
```

See the product documentation for more information.

### Configuration
A predefined configuration is provided for the two projects that use them:
* [YuniKorn k8shim](https://github.com/cloudera/yunikorn-k8shim)
* [YuniKorn core](https://github.com/cloudera/yunikorn-core).  

The configuration is provided in the yaml file: [golangci.yml](../.golangci.yml).

The web interface is a javascript project and the scheduler interface only has generated Go code and thus do not use it. 

### Integration in pull requests
The planning is to integrate the `golangci-lint` check into the GitHub PR flow. 

## False positives
Tools are never 100% correct and neither is this one.
Certain issue are too hard to correct or are not important enough to fix.

The tool allows adding a comment to the code to ignore the issue.
These comments should be used sparingly as they could hide issues.
If they are used they should be accompanied by a comment to explain why they are used.
```go
	var s1 = "ignored by all linters" //nolint
	var s2 = "ignored by the linter unused" //nolint:unused
``` 
Using the `nolint` comment without a specific linter is discouraged.  

