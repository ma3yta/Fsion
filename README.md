# Fsion

[![Linux Build](https://travis-ci.org/AnthonyLloyd/Fsion.svg?branch=master)](https://travis-ci.org/AnthonyLloyd/Fsion)
[![Windows Build](https://ci.appveyor.com/api/projects/status/qcpmg6thnmwe09tn/branch/master?svg=true)](https://ci.appveyor.com/project/AnthonyLloyd/Fsion)
[![NuGet Badge](https://buildstats.info/nuget/Fsion)](https://www.nuget.org/packages/Fsion)

EAVT (Entity, Attribute, Value, Time) database for F#

Vaporware currently but here is an outline.

## Key ideas

- Full bi-temporal historic and audit querying like [Datomic](https://www.datomic.com/) but with (Business Date, Value, Transaction Id) compressed time-series.
- Embedded - server (or client) can perform typed queries in memory. Bye-bye sending strings to a database machine.
- Embedded - scale up rather than out using in memory querying and local disk. Much simpler and cheaper $/perf.
- Embedded - in memory or memory and disk using [FASTER](https://github.com/Microsoft/FASTER).
- Native Attributes - richer metadata: F# types, validation, display, localization, powerful F# custom aggregation.
- Native Attributes - F# function attributes (persist only real datums), saves memory and disk, think DAG.
- Transactions - just a normal entity, attach custom context attributes.
- Transactions - persist to cloud and local storage.
- Transactions - send to clients or replicates, easier what-if functionality.
- Views - F# functions that take unstructured datums to fully type safe data structures.
- Indexes - like [Datomic](https://docs.datomic.com/cloud/query/raw-index-access.html) (hidden, automatic) EAVT, AEVT, VAET etc.
- API - binary Socket, Linq, Excel.
- API - think more GraphQL and XPath rather than SQL.
- Not Datomic - time-series compression.
- Not Datomic - entities do have a type. I think this will lead to type safe relations, faster query and better compression.
- Not Datomic - not SQL based, either typed in memory query or simple XPath/GraphQL expressions.

Having built parts of this multiple times I want to create a high-quality OSS version and build on it.

Happy to discuss further in the issues section. Welcome anyone else coming on board.

## Roadmap

- Set up the repo (build, CI etc) - DONE
- DataSeries with compression - DONE
- Define internal APIs (DB, Log) with in memory and disk implementations - October?
- Attribute metadata and functionality - November?
- Indexes - November?
- Simple Excel API - December?
- Large sample database
- Other internal API implementations
- Performance and resilience tests
- Query language
- Other external APIs
- Views
- Satellite projects
    - Data mapping
    - Reconciliation
    - Permissions
    - Scheduling
    - Accounting attribute functions with aggregation