module Fsion.Tests.MapSlimTests

open Expecto
open Fsion

let mapSlimTests =

    testList "mapslim" [

        testAsync "tryget none" {
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

        testAsync "single entry" {
            let ms = MapSlim()
            let x = &ms.RefGet 7
            x <- x + 1
            let x = &ms.RefGet 7
            x <- x + 3
            Expect.equal (ms.TryGet 7) (Some 4) "single"
        }
    ]
