module Fsion.Tests.ListSlimTests

open System
open System.Collections.Generic
open Expecto
open Fsion

type KeyWithHash =
    val key : uint64
    val hashCode : int
    new(i:uint64, hashCode: int) = { key = i; hashCode = hashCode }
    interface IEquatable<KeyWithHash> with
        member m.Equals (o:KeyWithHash) =
            m.key = o.key
    override m.Equals(o:obj) =
        o :? KeyWithHash && (o :?> KeyWithHash).key = m.key
    override m.GetHashCode() = m.hashCode

let unitTests =

    testList "unit tests" [

        testAsync "count" {
            let ls = ListSlim()
            for i = 99 downto 0 do
                ls.Add i |> ignore
            Expect.equal ls.Count 100 "count"
        }

        testAsync "item" {
            let ls = ListSlim()
            ls.Add 5 |> ignore
            ls.Add 3 |> ignore
            Expect.equal ls.[0] 5 "item 0"
            Expect.equal ls.[1] 3 "item 1"
        }
    ]

let performanceTests =

    testList "performance" [

        testSequenced <| ptestAsync "set" {
            let keys =
                let size, aggCount = 10_000, 250
                let rand = Random 577656
                let hashCode = memoize (fun _ -> rand.Next())
                Array.init size (fun _ ->
                    let i = uint64 (rand.Next(size/aggCount))
                    KeyWithHash(i, hashCode i)
                )
            Expect.isFasterThan
                (fun () ->
                    let ls = ListSlim()
                    for i = 0 to keys.Length-1 do
                        let k = keys.[i]
                        ls.Add k |> ignore
                )
                (fun () ->
                    let ls = List()
                    for i = 0 to keys.Length-1 do
                        let k = keys.[i]
                        ls.Add k
                )
                "listslim add"
        }

        testSequenced <| ptestAsync "get" {
            let n = 10_000
            let ls = ListSlim()
            let l = List()
            let rand = Random 46576
            let hashCode = memoize (fun _ -> rand.Next())
            let keys = Array.init n (fun i ->
                let i = uint64 i
                KeyWithHash(i, hashCode i)
            )
            for i = 0 to n-1 do
                let k = keys.[i]
                ls.Add k |> ignore
                l.Add k
            Expect.isFasterThan
                (fun () ->
                    for i = 0 to n-1 do
                        ls.[i] |> ignore
                )
                (fun () ->
                    for i = 0 to n-1 do
                        l.[i] |> ignore
                )
                "listslim get"
        }
    ]

let threadingTests =
    let ls = ListSlim()
    let rand = Random 46475
    let n = 10
    testList "threading" [
        
        testAsync "rand update" {
            lock ls (fun () ->
                let i = rand.Next n
                if i < ls.Count then
                    ls.[i] <- 1 - ls.[i]
                else
                    ls.Add 0 |> ignore
            )
        }

        testAsync "rand item" {
            if ls.Count <> 0 then
                let i = rand.Next ls.Count
                let k = ls.[i]
                Expect.isTrue (k=0 || k=1) "value is 0 or 1"
        }
    ]

let listSlimTests =
    testList "listslim" [
        unitTests
        performanceTests
        threadingTests
    ]