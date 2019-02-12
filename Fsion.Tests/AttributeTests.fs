module Fsion.Tests.AttributeTests

open System
open Expecto
open Fsion

open Selector

let attributeValidUriTestList =
    let isValid s =
        let isValid =
            match Text s,0,s.Length with
            | UriUri -> true
            | _ -> false
        Expect.isTrue isValid ("isValid "+s)
    let isNotValid s =
        let isValid =
            match Text s,0,s.Length with
            | UriUri -> true
            | _ -> false
        Expect.isFalse isValid ("isNotValid "+s)
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
        attributeValidUriTestList
    ]