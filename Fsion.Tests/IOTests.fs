module Fsion.Tests.IOTests

open System.IO
open Fsion

type ConsoleService =
    abstract member WriteLine : string -> IO<'r,unit>
    abstract member ReadLine : unit -> IO<'r,string,IOException>

type Console =
    abstract member Console : ConsoleService

module Console =
    let writeLine s = io.Effect(fun (c:#Console) -> c.Console.WriteLine s)
    let readLine() = io.Effect(fun (c:#Console) -> c.Console.ReadLine())

type LoggingService =
    abstract member Log : string -> IO<'r,unit>

type Logger =
    abstract member Logging : LoggingService

module Logger =
    let log s = io.Effect(fun (l:#Logger) -> l.Logging.Log s)

type PersistenceService =
    abstract member Persist : 'a -> IO<'r,unit,IOException>

type Persistence =
    abstract member Persistence : PersistenceService

module Persistence =
    let persist a = io.Effect(fun (p:#Persistence) -> p.Persistence.Persist a)



let testConsole =
    { new ConsoleService with
        member __.WriteLine s =
            IO<_,_>.Pure (fun _ -> printfn "%s" s)
        member __.ReadLine() =
            IO<_,_,_>.Pure (fun _ -> Ok "Hi")
    }

let testLogging =
    { new LoggingService with
        member __.Log s =
            IO<_,_>.Pure (fun _ -> printfn "Log: %s" s)
    }

let testPersistence =
    { new PersistenceService with
        member __.Persist a =
            IO<_,_,_>.Pure (fun _ ->
                printfn "Saved: %A" a
                Ok ()
            )
    }

type TestEnv() =
    interface Console with member __.Console = testConsole
    interface Logger with member __.Logging = testLogging
    interface Persistence with member __.Persistence = testPersistence

let program() =
    io {
        do! Logger.log "started"
        do! Console.writeLine "Please enter your name:"
        let! name = Console.readLine()
        do! Logger.log ("got name = " + name)
        do! Persistence.persist name
        do! Console.writeLine ("Hi "+name)
        do! Logger.log "finished"
        return 0
    }

let test() =
    program().Run(TestEnv())