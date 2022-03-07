[![codecov](https://codecov.io/gh/pityka/tasks/branch/master/graph/badge.svg)](https://codecov.io/gh/pityka/tasks)
[![](https://github.com/pityka/tasks/workflows/CI/badge.svg)](https://github.com/pityka/tasks/actions)
[![maven](https://img.shields.io/maven-central/v/io.github.pityka/tasks-core_2.13.svg)](https://repo1.maven.org/maven2/io/github/pityka/tasks-core_2.13/)




This library provides persistent memoization around asynchronous distributed computations.

It also provides a framework to execute those computations on remote machines. For the latter it supports AWS EC2, kubernetes, or ssh.

## Example

See the `example` project and the tests.

# Licence

Author: Istvan Bartha

This repository is a fork of https://github.com/pityka/mybiotools/tree/master/Tasks , for which the copyright holder is `ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland, Group Fellay` and was released under the MIT license. 

Copyright of subsequent modifications belong to Istvan Bartha.

```
The MIT License (MIT)

Original work (as in https://github.com/pityka/mybiotools/tree/master/Tasks):
Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland, Group Fellay

Modified work (this repository): Copyright (c) 2016 Istvan Bartha

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
