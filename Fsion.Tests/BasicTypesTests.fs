module Fsion.Tests.BasicTypesTests

open System
open Expecto
open Fsion

let basicTypesTests =

    testList "basicTypes" [
        
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