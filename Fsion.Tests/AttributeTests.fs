module Fsion.Tests.AttributeTests

open System
open Expecto
open Fsion

let valueTypeTestList =

    let testRoundtrip encode decode expected =
        let actual = encode expected |> decode
        Expect.equal actual expected "attribute roundtrip"

    testList "valuetype" [

        testProp "bool roundtrip" (
            testRoundtrip FsionValue.encodeBool FsionValue.decodeBool
        )
        
        testProp "int roundtrip" (
            testRoundtrip FsionValue.encodeInt FsionValue.decodeInt
        )

        testProp "int64 roundtrip" (
            testRoundtrip FsionValue.encodeInt64 FsionValue.decodeInt64
        )

        testProp "uri roundtrip" (
            testRoundtrip FsionValue.encodeUri FsionValue.decodeUri
        )

        testProp "date roundtrip" (
            testRoundtrip FsionValue.encodeDate FsionValue.decodeDate
        )

        testProp "time roundtrip" (
            testRoundtrip FsionValue.encodeTime FsionValue.decodeTime
        )

        testProp "textid roundtrip" (
            testRoundtrip FsionValue.encodeTextId FsionValue.decodeTextId
        )

        testProp "dataid roundtrip" (
            testRoundtrip FsionValue.encodeDataId FsionValue.decodeDataId
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