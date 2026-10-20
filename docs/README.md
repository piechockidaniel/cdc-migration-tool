# Documentation

Two guides, one reference. Everything else in this folder is either screenshots or history.

## For operators, data engineers and architects – `user-guide/`

|                                                                                | Read when you…                                                                                       |
|--------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| [1. Getting started](user-guide/01-getting-started.md)                         | install the tool, try it with the built-in demo engine, run your first pipeline                      |
| [2. The web UI](user-guide/02-ui.md)                                           | want to know what every page does                                                                    |
| [3. Connections & CDC prerequisites](user-guide/03-connections.md)             | connect SQL Server, PostgreSQL, MariaDB/MySQL, MongoDB or Oracle and prepare them for change capture |
| [4. Mappings & transformations](user-guide/04-mappings.md)                     | define how tables/columns are reshaped, transformed and keyed                                        |
| [5. Pipelines & snapshots](user-guide/05-pipelines-and-snapshots.md)           | run continuous replication and initial loads – including the multi-TB table playbook                 |
| [6. Monitoring, failures & alerting](user-guide/06-monitoring-and-alerting.md) | watch lag and throughput, work the dead-letter queue, tune rule sets and channels                    |
| [7. Operations runbook](user-guide/07-operations-runbook.md)                   | deploy, back up, size retention, handle incidents                                                    |

## For developers – `developer-guide/`

|                                                                               |                                                                               |
|-------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| [1. Architecture](developer-guide/01-architecture.md)                         | components, data flow, delivery guarantees, the design decisions and why      |
| [2. Connectors](developer-guide/02-connectors.md)                             | the `SourceConnector` contract, per-engine internals, how to add an engine    |
| [3. Storage](developer-guide/03-storage.md)                                   | why files were replaced by a transactional store; the `ConfigStore` interface |
| [4. Audit, rules & alerting internals](developer-guide/04-audit-internals.md) | audit schema, signature normalisation, classifier, safe expression evaluator  |
| [5. Frontend](developer-guide/05-frontend.md)                                 | SPA structure, conventions, browser tests, legacy pages                       |
| [6. Testing](developer-guide/06-testing.md)                                   | unit / API / live / perf / UI test layers and how to run them                 |
| [7. History](developer-guide/07-history.md)                                   | the 1.x code review, the IDE inspection triage and the 2.0 change log         |

## Reference – `reference/`

* [Configuration reference](reference/configuration.md) – environment variables, connection documents per engine, mapping / pipeline / snapshot / rule schemas, API endpoint map.

The interactive API documentation is served by the application at `/docs` (Swagger) and `/redoc`.
