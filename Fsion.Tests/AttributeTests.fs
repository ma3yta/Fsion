module Fsion.Tests.AttributeTests

open System
open Expecto
open Fsion

let valueTypeTestList =

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

let attributeValidNameTestList =
    let isValid s =
        Expect.isTrue (Text s |> Selector.validateName) ("isValid "+s)
    let isNotValid s =
        Expect.isFalse (Text s |> Selector.validateName) ("isNotValid "+s)
    testList "validate name" [
        
        testAsync "valid" {
            isValid "f"
            isValid "hel"
            isValid "one_two"
            isValid "one_2"
            isValid "o1e"
            isValid "one1_fred2"
            isValid "one_two_three"
        }
    
        testAsync "not valid" {
            isNotValid "_hi"
            isNotValid "hi_"
            isNotValid "1hi"
            isNotValid "one__two"
            isNotValid "oNe"
        }
    ]

let attributeTestList =
    testList "attribute" [
        valueTypeTestList
        attributeValidNameTestList
    ]