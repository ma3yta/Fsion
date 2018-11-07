module Fsion.Tests.AttributeTests

open System
open Expecto
open Fsion

let attributeTestList =

    let testRoundtrip valueType expected =
        let actual = valueType.ToInt expected |> valueType.OfInt
        Expect.equal actual expected "attribute roundtrip"

    testList "valuetype" [

        testProp "bool roundtrip" (
            testRoundtrip ValueType.Bool
        )
        
        testProp "int roundtrip" (
            testRoundtrip ValueType.Int
        )

        testProp "int64 roundtrip" (
            testRoundtrip ValueType.Int64
        )

        testProp "uri roundtrip" (
            testRoundtrip ValueType.Uri
        )

        testProp "date roundtrip" (
            testRoundtrip ValueType.Date
        )

        testProp "time roundtrip" (
            testRoundtrip ValueType.Time
        )

        testProp "textid roundtrip" (
            testRoundtrip ValueType.TextId
        )

        testProp "dataid roundtrip" (
            testRoundtrip ValueType.DataId
        )
    ]
