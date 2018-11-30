module Fsion.Tests.MapSlimTests

open System
open System.Collections.Generic
open Expecto
open Fsion

let mapSlimTests =

    //let memoizeDictSlim (f:'a->'b) =
    //    let d = DictSlim()
    //    fun a ->
    //        let mutable b = Unchecked.defaultof<_>
    //        if d.GetOrAddValueRef2(a, &b) then b
    //        else
    //            b <- f a
    //            b

    //let memoizeSlim (f:'a->'b) =
    //    let d = MapSlim()
    //    fun a ->
    //        let mutable isNew = false
    //        let b = &d.RefGet(a, &isNew)
    //        if isNew then b <- f a
    //        b

    testList null [
        testList "mapslim" [

            testAsync "tryget none" {
                let ms = MapSlim()
                Expect.equal (ms.TryGet 8) None "none"
            }

            testAsync "tryget none 2" {
                let ms = MapSlim()
                let x = &ms.RefGet 7
                x <- x + 1
                Expect.equal (ms.TryGet 8) None "none"
            }

            testAsync "tryget some" {
                let ms = MapSlim()
                let x = &ms.RefGet 7
                x <- 11
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

            testAsync "refget 100" {
                let ms = MapSlim()
                for i = 0 to 99 do
                    let x = &ms.RefGet i
                    x <- i
                Expect.equal (ms.Count) 100 "100"
            }
        ]

        testList "mapslim perf" [

            testSequenced <| testAsync "general dict" {
                let keys =
                    let size = 10_000
                    let aggCount = 250
                    let rand = Random(11231992)
                    Array.init size (fun _ -> rand.Next(size/aggCount))
                Expect.isFasterThan
                    (fun () ->
                        let refDict = DictSlim()
                        for i = 0 to keys.Length-1 do
                            let k = keys.[i]
                            let v = &refDict.GetOrAddValueRef(k)
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
                    "slim faster dict"
            }

            testSequenced <| testAsync "general" {
                let keys =
                    let size = 10_000
                    let aggCount = 250
                    let rand = Random 11231992
                    Array.init size (fun _ -> rand.Next(size/aggCount))
                Expect.isFasterThan
                    (fun () ->
                        let refDict = MapSlim()
                        for i = 0 to keys.Length-1 do
                            let k = keys.[i]
                            let v = &refDict.RefGet(k)
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
                    "slim faster"
            }
        ]

        testList "mapslim threading" [
            
        ]

    ]