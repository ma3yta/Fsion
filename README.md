# Fsion

[![Linux Build](https://travis-ci.org/AnthonyLloyd/Fsion.svg?branch=master)](https://travis-ci.org/AnthonyLloyd/Fsion)
[![Windows Build](https://ci.appveyor.com/api/projects/status/qcpmg6thnmwe09tn/branch/master?svg=true)](https://ci.appveyor.com/project/AnthonyLloyd/Fsion)
[![NuGet Badge](https://buildstats.info/nuget/Fsion)](https://www.nuget.org/packages/Fsion)

EAVT (Entity, Attribute, Value, Time) database for F#

Vaporware at the moment but here is an outline.

## Key ideas

- Full bi-temporal historic and audit querying similar to [Datomic](https://www.datomic.com/) but with (Business Date, Value, Transaction Id) compressed time-series.
- Embedded - server (or client) can perform typed queries in memory. Bye bye sending strings to a database machine.
- Embedded - scale up rather than out using in memory querying and local disk. Much simpler and cheaper $/perf.
- Embedded - in memory or memory and disk using [FASTER](https://github.com/Microsoft/FASTER).
- Native Attributes - richer metadata: F# types, display, name localization, powerful custom aggregation.
- Native Attributes - function attributes, record only real datums, saves memory and disk, think DAG.
- Transactions - just a normal entity, attach custom context attributes.
- Transactions - persist to cloud and local storage.
- Transactions - send to clients or replicates, easier what-if functionality.
- Indexes - Similar to [Datomic](https://docs.datomic.com/cloud/query/raw-index-access.html) (hidden, automatic) EAVT, AEVT, VAET etc.
- API - binary Socket, Linq, Excel.
- API - think more GraphQL and XPath rather than SQL.
- Not Datomic - time-series compression.
- Not Datomic - entities do have a type. I think this will lead to more type safe relations and better compression.
- Not Datomic - not SQL based, either typed in memory query or simple XPath/GraphQL expressions.
