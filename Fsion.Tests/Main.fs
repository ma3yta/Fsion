module Fsion.Tests.Main

open System
open Expecto
let tests =
    testList null [
        TypesTests.bytePoolTests
        TypesTests.basicTypesTests
        SerializeTests.zigzagTests
        SerializeTests.arraySerializeTests
        SerializeTests.dataSeriesTests
        SerializeTests.streamSerializeTests
        DatabaseTests.dataCacheTests
        DatabaseTests.databaseTests
    ]

[<EntryPoint;STAThread>]
let main args =
    let writeResults =
        TestResults.writeNUnitSummary
            ("bin/Fsion.Tests.TestResults.xml", "Fsion.Tests")
    let config = defaultConfig.appendSummaryHandler writeResults
    let r = runTestsWithArgs config args tests
    runAfterTesting()
    r