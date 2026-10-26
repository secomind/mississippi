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

## [1.0.1] - Unreleased
### Added
- Allow configuring the `fullsweep_after` process flag on AMQPDataConsumer processes via the
  `:fullsweep_after` queue option, to avoid accumulating uncollected binaries across many minor garbage collections before a full sweep runs. Defaults to 20.
- Allow tuning the distribution of the EventsProducer workers across the available nodes.
  The new option `worker_distribution_scope` can be set to:
  - `local`: the workers handling the incoming messages are located on the same node receiving them;
    guaranteed per-producer in-order delivery
  - `cluster` (default option): the workers handling the incoming messages are distributed across
    the whole cluster of nodes; guaranteed per-queue in-order delivery

### Changed
- `MessageTracker.get_message_tracker/1` and `DataUpdater.get_data_updater_process/1` now try a
  `Horde.Registry` lookup first, only falling back to `Horde.DynamicSupervisor.start_child/2` when
  the process is not already registered.

## [1.0.0] - 2025-02-07
### Added
- First Mississippi release.
