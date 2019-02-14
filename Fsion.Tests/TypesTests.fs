module Fsion.Tests.TypesTests

open System
open Expecto
open Fsion

let basicTypesTests =

    let isUriValid s =
        let isValid =
            match Text s,0,s.Length with
            | UriUri -> true
            | _ -> false
        Expect.isTrue isValid ("isValid "+s)
    let isUriNotValid s =
        let isValid =
            match Text s,0,s.Length with
            | UriUri -> true
            | _ -> false
        Expect.isFalse isValid ("isNotValid "+s)

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
                let actual = Text.ofString expected |> Option.get |> Text.toString
                let same = LanguagePrimitives.PhysicalEquality actual expected
                Expect.isTrue same "same"
            }
        ]

        testList "uri" [
            
            testAsync "valid" {
                isUriValid "f"
                isUriValid "hel"
                isUriValid "one_two"
                isUriValid "one_2"
                isUriValid "o1e"
                isUriValid "one1_fred2"
                isUriValid "one_two_three"
            }
        
            testAsync "not valid" {
                isUriNotValid "_hi"
                isUriNotValid "hi_"
                isUriNotValid "1hi"
                isUriNotValid "one__two"
                isUriNotValid "oNe"
            }
        ]
    ]