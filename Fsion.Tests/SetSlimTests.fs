module Fsion.Tests.SetSlimTests

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

    let getRefGenericTest (items: 'a list, check: 'a) =
        let actual =
            let ss = SetSlim()
            List.iter (ss.Add >> ignore) items
            ss.Get check
            |> VOption.isSome

        let expected =
            List.fold (fun m k -> Set.add k m) Set.empty items
            |> Set.contains check

        Expect.equal actual expected "group by"

    let groupByGenericTest (items : 'a list) =
        let actual =
            let ms = SetSlim()
            List.iter (ms.Add >> ignore) items
            List.init ms.Count ms.Item
            |> List.sort

        let expected =
            List.distinct items
            |> List.sort

        Expect.equal actual expected "group by"

    testList "unit tests" [

        testAsync "get none" {
            let ss = SetSlim()
            Expect.equal (ss.Get 8) ValueNone "none"
        }

        testAsync "get one none" {
            let ss = SetSlim()
            ss.Add 7 |> ignore
            Expect.equal (ss.Get 8) ValueNone "none"
        }

        testAsync "get one some" {
            let ss = SetSlim()
            let x = ss.Add 7
            Expect.equal (ss.Get 7) (ValueSome x) "some"
        }

        testAsync "same hashcode" {
            let ss = SetSlim()
            let key1 = KeyWithHash(7UL, 3)
            let i1 = ss.Add key1
            let key2 = KeyWithHash(19UL, 3)
            let i2 = ss.Add key2
            let key3 = KeyWithHash(27UL, 3)
            let i3 = ss.Add key3
            Expect.equal (ss.Get key1) (ValueSome i1) "key1"
            Expect.equal (ss.Get key2) (ValueSome i2) "key2"
            Expect.equal (ss.Get key3) (ValueSome i3) "key3"
        }

        testAsync "count" {
            let ss = SetSlim()
            for i = 99 downto 0 do
                ss.Add i |> ignore
            Expect.equal ss.Count 100 "count"
        }

        testAsync "item" {
            let ss = SetSlim()
            ss.Add 5 |> ignore
            ss.Add 3 |> ignore
            Expect.equal (ss.Item 0) 5 "item 0"
            Expect.equal (ss.Item 1) 3 "item 1"
        }

        testProp "get ref uint32"
            (getRefGenericTest : uint32 list * uint32 -> unit)

        testProp "get ref char"
            (getRefGenericTest : char list * char -> unit)

        testProp "get ref text"
            (getRefGenericTest : Text list * Text -> unit)

        testProp "group by uint32"
            (groupByGenericTest : uint32 list -> unit)

        testProp "group by char"
            (groupByGenericTest : char list -> unit)

        testProp "group by text"
            (groupByGenericTest : Text list -> unit)
    ]

let performanceTests =

    testList "performance" [

        testSequenced <| testAsync "set" {
            let keys =
                let size, aggCount = 5000, 250
                let rand = Random 577656
                let hashCode = memoize (fun _ -> rand.Next())
                Array.init size (fun _ ->
                    let i = uint64 (rand.Next(size/aggCount))
                    KeyWithHash(i, hashCode i)
                )
            Expect.isFasterThan
                (fun () ->
                    let ss = SetSlim()
                    for i = 0 to keys.Length-1 do
                        let k = keys.[i]
                        ss.Add k |> ignore
                )
                (fun () ->
                    let hs = HashSet()
                    for i = 0 to keys.Length-1 do
                        let k = keys.[i]
                        hs.Add k |> ignore
                )
                "setslim add"
        }

        testSequenced <| testAsync "get" {
            let n = 5000
            let ss = SetSlim()
            let hs = HashSet()
            let rand = Random 46576
            let hashCode = memoize (fun _ -> rand.Next())
            let keys = Array.init n (fun i ->
                let i = uint64 i
                KeyWithHash(i, hashCode i)
            )
            for i = 0 to n-1 do
                let k = keys.[i]
                ss.Add k |> ignore
                hs.Add k |> ignore
            Expect.isFasterThan
                (fun () ->
                    for i = 0 to n-1 do
                        ss.Get(keys.[i]) |> ignore
                )
                (fun () ->
                    for i = 0 to n-1 do
                        hs.Contains(keys.[i]) |> ignore
                )
                "setslim get"
        }
    ]

let threadingTests =
    let ss = SetSlim()
    let rand = Random 46475
    let n = 10
    testList "threading" [
        
        testAsync "rand update" {
            lock ss (fun () ->
                ss.Add(rand.Next n) |> ignore
            )
        }

        testAsync "rand get" {
            let i = rand.Next n
            let v = defaultValueArg (ss.Get i) -1
            Expect.isTrue (v >= -1 && v < n) "get -1 or less n"
        }

        testAsync "rand item" {
            let i = rand.Next ss.Count
            let k = ss.Item i
            Expect.isLessThan k n "key is ok"
        }
    ]

let setSlimTests =
    testList "setslim" [
        unitTests
        performanceTests
        threadingTests
    ]