module Fsion.Tests.TypesTests

open System
open Expecto
open Fsion

let bytePoolTests =

    testList "bytePool" [

        testAsync "resize needed" {
            let expected = BytePool.Rent 512
            let actual = BytePool.ResizeUp expected 513
            let same = LanguagePrimitives.PhysicalEquality expected actual
            Expect.isFalse same "not same"
        }

        testAsync "resize not needed" {
            let expected = BytePool.Rent 512
            let actual = BytePool.ResizeUp expected 512
            let same = LanguagePrimitives.PhysicalEquality expected actual
            Expect.isTrue same "same"
        }

        testAsync "resize exact" {
            let expected = BytePool.Rent 512
            let actual = BytePool.ResizeExact (expected,511)
            Expect.equal actual.Length 511 "smaller"
        }

        testAsync "different" {
            let getBytes = async { return BytePool.Rent 128 }
            let! byteArray = Async.Parallel [|getBytes;getBytes;getBytes|]
            for i = 0 to 2 do
                for j = i+1 to 2 do
                    let same = LanguagePrimitives.PhysicalEquality byteArray.[i] byteArray.[j]
                    Expect.isFalse same "same"
            Array.iter BytePool.Return byteArray
        }

        testProp "big enough" (fun i ->
            let bytes = BytePool.Rent i
            Expect.isGreaterThanOrEqual bytes.Length i "large than i"
        )
    ]

let basicTypesTests =

    testList "types" [
        
        testList "text" [
        
            testAsync "null and whitespace" {
                Expect.equal (Text.ofString null) (Text.ofString String.Empty) "null"
                Expect.equal (Text.ofString " ") (Text.ofString String.Empty) "ws"
                Expect.equal (Text.ofString "  ") (Text.ofString String.Empty) "ws2"
            }

            testAsync "trim" {
                Expect.equal (Text.ofString " hello ") (Text.ofString "hello") "trim"
            }

            testAsync "trim same string" {
                let expected = "my string"
                let actual = Text.ofString expected |> Text.toString
                let same = LanguagePrimitives.PhysicalEquality actual expected
                Expect.isTrue same "same"
            }
        ]
    ]