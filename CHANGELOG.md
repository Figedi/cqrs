# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

### [1.3.3](https://github.com/Figedi/cqrs/compare/v1.3.2...v1.3.3) (2021-08-05)


### Bug Fixes

* allows event-type for status query ([3538b1a](https://github.com/Figedi/cqrs/commit/3538b1aeb52a9436102e4295c0ab334bc4fae4ee))

### [1.3.2](https://github.com/Figedi/cqrs/compare/v1.3.1...v1.3.2) (2021-07-29)


### Bug Fixes

* status w/ multiple eventIds/streamIds ([c3f544a](https://github.com/Figedi/cqrs/commit/c3f544a8b929219bbb0da598337ddd9ae805803a))

### [1.3.1](https://github.com/Figedi/cqrs/compare/v1.3.0...v1.3.1) (2021-07-28)


### Bug Fixes

* adds status hook to cqrs core module ([3e758b4](https://github.com/Figedi/cqrs/commit/3e758b42a73b86cab12e89505b423b0aac80a7a6))

## [1.3.0](https://github.com/Figedi/cqrs/compare/v1.2.1...v1.3.0) (2021-04-05)


### Features

* adds event scheduler ([595f9a2](https://github.com/Figedi/cqrs/commit/595f9a246609ca4a361ed32460de010f43edb549))
* adds exponential backoff retry strategy ([5a32c2b](https://github.com/Figedi/cqrs/commit/5a32c2bc2d7557f3872e9e43af50fc1197b92ae2))


### Bug Fixes

* adds missing assertings in spec ([4cea3b6](https://github.com/Figedi/cqrs/commit/4cea3b6e033d13c020c035e0772ad2b93e93e7c2))

### [1.2.1](https://github.com/Figedi/cqrs/compare/v1.2.0...v1.2.1) (2021-01-14)


### Bug Fixes

* adds sagaTriggeredEvent, adds transient for querybus ([1e74557](https://github.com/Figedi/cqrs/commit/1e745574548f91323bce841119124fec3c398e4a))

## [1.2.0](https://github.com/Figedi/cqrs/compare/v1.1.8...v1.2.0) (2020-12-19)


### Features

* adds possibility to execute events/queries/commands via pre-defined ids ([f42a5b4](https://github.com/Figedi/cqrs/commit/f42a5b4dfadf347e1f7b40069ef9264e98c90b6f))

### [1.1.8](https://github.com/Figedi/cqrs/compare/v1.1.7...v1.1.8) (2020-12-03)


### Bug Fixes

* fixes deserialization of commands ([25d657d](https://github.com/Figedi/cqrs/commit/25d657d2190c580b61f3cb27cea1daf85fd0eca5))

### [1.1.7](https://github.com/Figedi/cqrs/compare/v1.1.6...v1.1.7) (2020-12-01)


### Bug Fixes

* fixes error logging statements, adds deserialize methods ([8851909](https://github.com/Figedi/cqrs/commit/885190952e9caa33f6474223b145692b0386a486))

### [1.1.6](https://github.com/Figedi/cqrs/compare/v1.1.5...v1.1.6) (2020-11-24)


### Bug Fixes

* adds transient command handling, serializes errors correctly ([189f913](https://github.com/Figedi/cqrs/commit/189f91319ed5f1f4cd41cda747caf860dac689f2))

### [1.1.5](https://github.com/Figedi/cqrs/compare/v1.1.4...v1.1.5) (2020-11-18)


### Bug Fixes

* adds utility to inject EventEntity into outer ormConfig ([2c76f79](https://github.com/Figedi/cqrs/commit/2c76f7985b06a54e78a473b5b9e3624ddeafddaf))

### [1.1.4](https://github.com/Figedi/cqrs/compare/v1.1.3...v1.1.4) (2020-11-17)


### Bug Fixes

* passes down connection via scopeProvider ([eba022f](https://github.com/Figedi/cqrs/commit/eba022f3082ec00abcaa62899c2ffdef71112253))

### [1.1.3](https://github.com/Figedi/cqrs/compare/v1.1.2...v1.1.3) (2020-11-16)


### Bug Fixes

* makes typeorm connection related stuff lazy ([122e724](https://github.com/Figedi/cqrs/commit/122e724505a3f6a7f04d2247253c78160461bd65))

### [1.1.2](https://github.com/Figedi/cqrs/compare/v1.1.1...v1.1.2) (2020-11-15)


### Bug Fixes

* adjusts root exports ([a890271](https://github.com/Figedi/cqrs/commit/a8902714ff726908b964bba9acfd5eb0efcbb810))

### [1.1.1](https://github.com/Figedi/cqrs/compare/v1.1.0...v1.1.1) (2020-11-15)


### Bug Fixes

* fixes main-path in package.json ([85a611f](https://github.com/Figedi/cqrs/commit/85a611f4a389fef5b8c294a8d10ca6c5fad59924))

## 1.1.0 (2020-11-15)


### Features

* initial commit ([c358ecd](https://github.com/Figedi/cqrs/commit/c358ecd467b3d66f396a7eb54a355fde383c5ebe))


### Bug Fixes

* adds docker-image for building ([c8a449e](https://github.com/Figedi/cqrs/commit/c8a449efc446e1e6c312d3d5b4433daf1068d429))
* fixes circleci config ([febaccc](https://github.com/Figedi/cqrs/commit/febaccc2edfdddc8dfc0724e8d4e1370f8e516d4))
