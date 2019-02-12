module Fsion.Tests.ValueTypeTests

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

        testProp "uint roundtrip" (
            testRoundtrip FsionValue.encodeUInt FsionValue.decodeUInt
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