module Fsion.Tests.Main

open System
open Expecto
let tests =
    testList null [
        SerializeTests.serializeTests
        SerializeTests.dataSeriesTests
        DatabaseTests.databaseTests
    ]

[<EntryPoint;STAThread>]
let main args =
    runTestsWithArgs defaultConfig args tests