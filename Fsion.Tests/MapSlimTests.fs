module Fsion.Tests.MapSlimTests

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

    let getRefGenericTest (items: ('a * int) list, check: 'a) =
        let actual =
            let ms = MapSlim()
            List.iter ms.Set items
            ms.GetOption check

        let expected =
            List.fold (fun m (k,v) -> Map.add k v m) Map.empty items
            |> Map.tryFind check
            |> VOption.ofOption

        Expect.equal actual expected "group by"

    let groupByGenericTest (items : ('a * int) list) =
        let actual =
            let ms = MapSlim()
            List.iter (fun (k,v) ->
                let t = &ms.GetRef k
                t <- t+v
            ) items
            List.init ms.Count ms.Item
            |> List.sort

        let expected =
            List.groupBy fst items
            |> List.map (fun (k,l) -> k, List.sumBy snd l)
            |> List.sort

        Expect.equal actual expected "group by"

    testList "unit tests" [

        testAsync "get none" {
            let ms = MapSlim()
            Expect.equal (ms.GetOption 8) ValueNone "none"
        }

        testAsync "get one none" {
            let ms = MapSlim()
            let x = &ms.GetRef 7
            x <- x + 1
            Expect.equal (ms.GetOption 8) ValueNone "none"
        }

        testAsync "get one some" {
            let ms = MapSlim()
            ms.Set(7,11)
            Expect.equal (ms.GetOption 7) (ValueSome 11) "some"
        }

        testAsync "same hashcode" {
            let ms = MapSlim()
            let key1 = KeyWithHash(7UL, 3)
            ms.Set(key1,11)
            let key2 = KeyWithHash(19UL, 3)
            ms.Set(key2,53)
            let key3 = KeyWithHash(27UL, 3)
            ms.Set(key3,99)
            Expect.equal (ms.GetOption key1) (ValueSome 11) "key1"
            Expect.equal (ms.GetOption key2) (ValueSome 53) "key2"
            Expect.equal (ms.GetOption key3) (ValueSome 99) "key3"
        }

        testAsync "getref update" {
            let ms = MapSlim()
            let x = &ms.GetRef 7
            x <- x + 1
            let x = &ms.GetRef 7
            x <- x + 3
            Expect.equal (ms.GetOption 7) (ValueSome 4) "update"
        }

        testAsync "count" {
            let ms = MapSlim()
            for i = 99 downto 0 do
                ms.Set(i,i)
            Expect.equal ms.Count 100 "count"
        }

        testAsync "item" {
            let ms = MapSlim()
            ms.Set(5,11)
            ms.Set(3,53)
            Expect.equal (ms.Item 0) (5,11) "item 0"
            Expect.equal (ms.Item 1) (3,53) "item 1"
        }

        testProp "get ref uint32"
            (getRefGenericTest : ((uint32 * int) list) * uint32 -> unit)

        testProp "get ref char"
            (getRefGenericTest : ((char * int) list) * char -> unit)

        testProp "get ref text"
            (getRefGenericTest : ((Text * int) list) * Text -> unit)

        testProp "group by uint32"
            (groupByGenericTest : (uint32 * int) list -> unit)

        testProp "group by char"
            (groupByGenericTest : (char * int) list -> unit)

        testProp "group by text"
            (groupByGenericTest : (Text * int) list -> unit)
    ]

let performanceTests =

    let memoizeOld (f:'a->'b) =
        let d = Dictionary HashIdentity.Structural
        fun a ->
            let mutable b = Unchecked.defaultof<_>
            if d.TryGetValue(a,&b) then b
            else
                b <- f a
                d.Add(a,b)
                b

    testList "performance" [

        testSequenced <| testAsync "set" {
            let keys =
                let size, aggCount = 5000, 250
                let rand = Random 11231992
                let hashCode = memoize (fun _ -> rand.Next())
                Array.init size (fun _ ->
                    let i = uint64 (rand.Next(size/aggCount))
                    KeyWithHash(i, hashCode i)
                )
            Expect.isFasterThan
                (fun () ->
                    let ms = MapSlim()
                    for i = 0 to keys.Length-1 do
                        let k = keys.[i]
                        let v = &ms.GetRef k
                        v <- v + 1
                )
                (fun () ->
                    let dict = Dictionary()
                    for i = 0 to keys.Length-1 do
                        let k = keys.[i]
                        let mutable t = Unchecked.defaultof<_>
                        if dict.TryGetValue(k, &t) then
                            dict.[k] <- t + 1
                        else
                            dict.Add(k,1)
                )
                "mapslim set"
        }

        testSequenced <| ptestAsync "get" {
            let n = 5000
            let ms = MapSlim()
            let dict = Dictionary()
            let rand = Random 11231992
            let hashCode = memoize (fun _ -> rand.Next())
            let keys = Array.init n (fun i ->
                let i = uint64 i
                KeyWithHash(i, hashCode i)
            )
            for i = 0 to n-1 do
                let k = keys.[i]
                ms.Set(k,k)
                dict.Add(k,k)
            Expect.isFasterThan
                (fun () ->
                    for i = 0 to n-1 do
                        ms.GetOption (keys.[i]) |> ignore
                )
                (fun () ->
                    for i = 0 to n-1 do
                        dict.TryGetValue(keys.[i]) |> ignore
                )
                "mapslim get"
        }

        testSequenced <| testAsync "memoize" {
            let n = 5000
            Expect.isFasterThan
                (fun () ->
                    let times2 = memoize ((*)2)
                    for i = 0 to n-1 do
                        times2 i |> ignore
                )
                (fun () ->
                    let times2 = memoizeOld ((*)2)
                    for i = 0 to n-1 do
                        times2 i |> ignore
                )
                "mapslim memoize"
        }
    ]

let threadingTests =
    let ms = MapSlim()
    let rand = Random 887363
    let n = 10
    testList "threading" [
        
        testAsync "rand update" {
            lock ms (fun () ->
                let v = &ms.GetRef(rand.Next n)
                v <- 1-v
            )
        }

        testAsync "rand get" {
            let i = rand.Next n
            let v = defaultValueArg (ms.GetOption i) 0
            Expect.isTrue (v=0 || v=1) "get is 0 or 1"
        }

        testAsync "rand item" {
            let i = rand.Next ms.Count
            let k,v = ms.Item i
            Expect.isLessThan k n "key is ok"
            Expect.isTrue (v=0 || v=1) "value is 0 or 1"
        }
    ]

let mapSlimTests =
    testList "mapslim" [
        unitTests
        performanceTests
        threadingTests
    ]