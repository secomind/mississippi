<!---
  Copyright 2024 SECO Mind Srl
  SPDX-License-Identifier: Apache-2.0
-->

# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [1.1.0] - Unreleased
### Added
- Support distributed Erlang clusters. Processes are now
  (automatically) sharded between all nodes.

### Fixed
- AMQPDataConsumers do not crash when a shutdown is requested.
- DataUpdaters and MessageTrackers are restarted only when requested.

## [1.0.0] - 2025-02-07
### Added
- First Mississippi release.
