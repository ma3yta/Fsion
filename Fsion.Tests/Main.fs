module Fsion.Tests.Main

open System
open Expecto
let tests =
    testList null [
        BasicTypesTests.basicTypesTests
        SerializeTests.serializeTests
        SerializeTests.dataSeriesTests
        DatabaseTests.dataSeriesTests
        DatabaseTests.textCacheTests
        DatabaseTests.dataSeriesBaseTests
    ]

[<EntryPoint;STAThread>]
let main args =
    let writeResults =
        TestResults.writeNUnitSummary
            ("bin/Fsion.Tests.TestResults.xml", "Fsion.Tests")
    let config = defaultConfig.appendSummaryHandler writeResults
    runTestsWithArgs config args tests