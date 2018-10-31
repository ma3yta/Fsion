namespace Fsion.Tests

open System
open System.IO
open Expecto
open FsCheck
open Fsion

module Gen =
    let floatArb = Arb.convert NormalFloat.op_Explicit NormalFloat Arb.from
    type Float01 = Float01 of float
    let float01Arb =
        let maxValue = float UInt64.MaxValue
        Arb.convert
            (fun (DoNotSize a) -> float a / maxValue |> Float01)
            (fun (Float01 f) -> f * maxValue + 0.5 |> uint64 |> DoNotSize)
            Arb.from
    let textArb = Arb.convert Text.ofString Text.toString Arb.from

    let addToConfig config = {
        config with
            arbitrary = typeof<Float01>.DeclaringType::config.arbitrary
    }

[<AllowNullLiteral>]
type TempDir (dir:DirectoryInfo) =
    member __.Path = dir.FullName
    member __.Dispose() =
        dir.Delete true

[<AutoOpen>]
module Auto =
    let private config = Gen.addToConfig FsCheckConfig.defaultConfig
    let testProp name =
        testPropertyWithConfig config name
    let ptestProp name =
        ptestPropertyWithConfig config name
    let ftestProp name =
        ftestPropertyWithConfig config name
    let etestProp stdgen name =
        etestPropertyWithConfig stdgen config name
    let tempDir () =
        let root = Path.GetTempPath () |> DirectoryInfo
        let dir = Path.GetRandomFileName() |> root.CreateSubdirectory
        TempDir dir
    let mutable private afterTestingList = []
    let mutable private afterTestingLock = obj()
    let afterTesting (f:unit->unit) =
        lock afterTestingLock (fun () ->
            afterTestingList <- f :: afterTestingList
        )
    let runAfterTesting () =
        lock afterTestingLock (fun () ->
            List.rev afterTestingList
            |> List.iter (fun f -> f())
            afterTestingList <- []
        )
