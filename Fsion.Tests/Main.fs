module Fsion.Tests.Main

open System
open System.IO
open Expecto

[<Struct>]
type Any = Any

[<NoEquality;NoComparison>]
type Env<'r,'e,'a> =
    | Env of ('r -> Result<'a,'e>)

module Env =
    let run s (Env run) =
        run s
    let unit a = Env (fun _ -> Ok a)
    let get<'s,'e> : Env<'s,'e,'s> = Env Ok
    //let set s = Env(fun _ -> Ok ())
    let map (f:'a->'b) (state:Env<'r,'e,'a>) =
        Env(fun s ->
            match run s state with
            | Ok a -> f a |> Ok
            | Error e -> Error e)
    let bind (f:'a->Env<'r,'e,'b>) (state:Env<'r,'e,'a>) =
        Env(fun s ->
            match run s state with
            | Ok a -> run s (f a)
            | Error e -> Error e
        )
    let bindFromAny (f:'a->Env<'r,'e,'b>) (state:Env<'r,Any,'a>) =
        Env(fun s ->
            match run s state with
            | Ok a -> run s (f a)
            | _ -> failwith "any"
        )
    let bindToAny (f:'a->Env<'r,Any,'b>) (state:Env<'r,'e,'a>) =
        Env(fun s ->
            match run s state with
            | Ok a ->
                match run s (f a) with
                | Ok b -> Ok b
                | _ -> failwith "any"
            | Error e -> Error e
        )
    let map2 s1 s2 f =
        bind (fun a -> map (fun b -> f a b) s2) s1

    type EnvBuilder<'e>() =
        member __.Bind(s:Env<'r,'e,'a>, f:'a -> Env<'r,Any,'b>) =
            bindToAny f s
        member __.Bind(s:Env<'r,Any,'a>, f:'a -> Env<'r,'e,'b>) =
            bindFromAny f s
        member __.Bind(s:Env<'r,'e,'a>, f:'a -> Env<'r,'e,'b>) =
            bind f s
        member __.Return(value) = unit value
        member __.ReturnFrom(value) = value
        member __.Yield(value) = unit value
        member __.Zero() = unit()
        member __.Combine(s1:Env<'s,'e,unit>, s2:Env<'s,'e,'a>) = map2 s1 s2 (fun _ s -> s)
        member __.Delay(f) = f()
        member __.For(xs:seq<'a>, f:'a -> Env<'s,'e,'a>) = xs |> Seq.map f
        member __.Run(value) = value

[<AutoOpen>]
module EnvAutoOpen =
    let env<'e> = Env.EnvBuilder<'e>()

type ConsoleService =
    abstract member WriteLine : string -> Env<'r,Any,unit>
    abstract member ReadLine : unit -> Env<'r,IOException,string>

type Console =
    abstract member Console : ConsoleService

module Console =
    let writeLine s = Env.bind (fun (c:#Console) -> c.Console.WriteLine s) Env.get
    let readLine() = Env.bind (fun (c:#Console) -> c.Console.ReadLine()) Env.get

type LoggingService =
    abstract member Log : string -> Env<'r,Any,unit>

type Logger =
    abstract member Logging : LoggingService

module Logger =
    let log s = Env.bind (fun (l:#Logger) -> l.Logging.Log s) Env.get

type PersistenceService =
    abstract member Persist : 'a -> Env<'r,IOException,unit>

type Persistence =
    abstract member Persistence : PersistenceService

module Persistence =
    let persist a = Env.bind (fun (p:#Persistence) -> p.Persistence.Persist a) Env.get

let program() =
    env<IOException> {
        do! Logger.log "started"
        do! Console.writeLine "Please enter your name:"
        let! name = Console.readLine()
        do! Logger.log ("got name = " + name)
        do! Persistence.persist name
        do! Console.writeLine ("Hi "+name)
        do! Logger.log "finished"
        return 0
    }



type TestEnv() =
    interface Console with
        member __.Console =
            { new ConsoleService with
                member __.WriteLine s =
                    Env.unit ()
                member __.ReadLine() =
                    Env.unit "hi"
            }
    interface Logger with
        member __.Logging =
            { new LoggingService with
                member __.Log s =
                    Env.unit ()
            }
    interface Persistence with
        member __.Persistence =
            { new PersistenceService with
                member __.Persist a =
                    Env.unit ()
            }

let test() =
    Env.run (TestEnv()) (program())
    


let tests =
    testList null [
        MapSlimTests.mapSlimTests
        SetSlimTests.setSlimTests
        ListSlimTests.listSlimTests
        BytePoolTests.bytePoolTests
        TypesTests.basicTypesTests
        SerializeTests.zigzagTests
        SerializeTests.arraySerializeTests
        SerializeTests.dataSeriesTests
        SerializeTests.streamSerializeTests
        ValueTypeTests.valueTypeTestList
        TransactorTests.transactorTests
        SelectorTests.dataCacheTests
    ]

[<EntryPoint;STAThread>]
let main args =
    let writeResults =
        TestResults.writeNUnitSummary
            ("bin/Fsion.Tests.TestResults.xml", "Fsion.Tests")
    let config = defaultConfig.appendSummaryHandler writeResults
    let r = runTestsWithArgs config args tests
    runAfterTesting()
    r