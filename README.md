# Golang Support Packages

[![MIT][License-Image]][License-Url] [![Go Report Card][ReportCard-Image]][ReportCard-Url] [![CICD](https://github.com/alwitt/go-utils/actions/workflows/cicd.yaml/badge.svg?branch=main)](https://github.com/alwitt/go-utils/actions/workflows/cicd.yaml)

[License-Url]: https://mit-license.org/
[License-Image]: https://img.shields.io/badge/License-MIT-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/alwitt/go-utils
[ReportCard-Image]: https://goreportcard.com/badge/github.com/alwitt/go-utils

Provides common support components used in other Golang projects.

## Components Provided

| Component | Description | Note |
|-----------|-------------|------|
| `Component` | Base `struct` for other components | Contains the support method `NewLogTagsForContext` which generates a new copy of the `apex` `logs.Fields` metadata structure. |
| `RestAPIHandler` | Base `struct` for other REST API handlers | Contains utility methods, and a REST request logging middleware. |
| `TaskProcessor` | An asynchronous job queue system | Provides single threaded and multi threaded worker pool implementation. |
| `IntervalTimer` | Task execution trigger interval timer | Operates in periodic or one-shot mode. |

## Getting Started

The provided [Makefile](Makefile) contains targets for various development actions. Starting out

```
make
```

to prepare the development and verify linters pass. Verify project is working with

```
make test
```

to execute all unit-tests.
