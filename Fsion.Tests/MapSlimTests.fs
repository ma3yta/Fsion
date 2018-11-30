module Fsion.Tests.MapSlimTests

open System
open System.Collections.Generic
open Expecto
open Fsion

let unitTests =
    
    let groupByGenericTest (items : ('a * int) list) =
        let ms = MapSlim()
        
        List.iter (fun (k,v) ->
            let t = &ms.RefGet k
            t <- t+v
        ) items

        let actual =
            List.init ms.Count ms.Item
            |> List.sort

        let expected =
            List.groupBy fst items
            |> List.map (fun (k,l) -> k, List.sumBy snd l)
            |> List.sort

        Expect.equal actual expected "group by"

    testList "unit tests" [

        testAsync "tryget none" {
            let ms = MapSlim()
            Expect.equal (ms.TryGet 8) None "none"
        }

        testAsync "tryget one none" {
            let ms = MapSlim()
            let x = &ms.RefGet 7
            x <- x + 1
            Expect.equal (ms.TryGet 8) None "none"
        }

        testAsync "tryget set" {
            let ms = MapSlim()
            ms.Set(7,11)
            Expect.equal (ms.TryGet 7) (Some 11) "some"
        }

        testAsync "refget update" {
            let ms = MapSlim()
            let x = &ms.RefGet 7
            x <- x + 1
            let x = &ms.RefGet 7
            x <- x + 3
            Expect.equal (ms.TryGet 7) (Some 4) "update"
        }

        testAsync "count" {
            let ms = MapSlim()
            for i = 99 downto 0 do
                ms.Set(i,i)
            Expect.equal (ms.Count) 100 "count"
        }

        testProp "group by uint32"
            (groupByGenericTest : (uint32 * int) list -> unit)

        testProp "group by char"
            (groupByGenericTest : (char * int) list -> unit)

        testProp "group by text"
            (groupByGenericTest : (Text * int) list -> unit)
        // TODO: number tests, item test, prop tests
    ]

let performanceTests =

    let memoizeOld (f:'a->'b) =
        let d = ResizeMap HashIdentity.Structural
        fun a ->
            let mutable b = Unchecked.defaultof<_>
            if d.TryGetValue(a,&b) then b
            else
                b <- f a
                d.Add(a,b)
                b

    testList "performance" [

        testSequenced <| testAsync "general" {
            let keys =
                let size, aggCount = 5_000, 250
                let rand = Random 11231992
                Array.init size (fun _ -> rand.Next(size/aggCount))
            Expect.isFasterThan
                (fun () ->
                    let refDict = MapSlim()
                    for i = 0 to keys.Length-1 do
                        let k = keys.[i]
                        let v = &refDict.RefGet k
                        v <- v + k
                )
                (fun () ->
                    let dict = Dictionary()
                    for i = 0 to keys.Length-1 do
                        let k = keys.[i]
                        let mutable t = Unchecked.defaultof<_>
                        if dict.TryGetValue(k, &t) then
                            dict.[k] <- t + k
                        else
                            dict.Add(k,k)
                )
                "mapslim general"
        }

        testSequenced <| testAsync "memoize" {
            Expect.isFasterThan
                (fun () ->
                    let times2 = memoize ((*)2)
                    for i = 0 to 99 do
                        times2 i |> ignore
                )
                (fun () ->
                    let times2 = memoizeOld ((*)2)
                    for i = 0 to 99 do
                        times2 i |> ignore
                )
                "mapslim memoize"
        }


        //testSequenced <| testAsync "general2" {
        //    let keys =
        //        let size, aggCount = 5_000, 250
        //        let rand = Random 11231992
        //        Array.init size (fun _ -> rand.Next(size/aggCount))
        //    Expect.isFasterThan
        //        (fun () ->
        //            let refDict = MapSlim2()
        //            for i = 0 to keys.Length-1 do
        //                let k = keys.[i]
        //                let v = &refDict.RefGet k
        //                v <- v + k
        //        )
        //        (fun () ->
        //            let refDict = MapSlim()
        //            for i = 0 to keys.Length-1 do
        //                let k = keys.[i]
        //                let v = &refDict.RefGet k
        //                v <- v + k
        //        )
        //        "mapslim general"
        //}

        // TODO: one more perf test
    ]

let threadingTests =
    let ms = MapSlim()
    let rand = Random 887363
    let n = 10
    testList "threading" [
        
        testAsync "rand update" {
            lock ms (fun () ->
                let v = &ms.RefGet(rand.Next n)
                v <- 1-v
            )
        }

        testAsync "rand get" {
            let i = rand.Next n
            let v = ms.TryGet i |> Option.defaultValue 0
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