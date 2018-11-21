namespace Fsion.Excel

open System
open Fsion
open ExcelDna.Integration

type private FsionFunction() =
    inherit ExcelFunctionAttribute(
        Category = "Fsion",
        IsExceptionSafe = true,
        IsThreadSafe = true
    )

module API =

    let selectorContext = Unchecked.defaultof<Selector.Context>
    
    [<Literal>]
    let private wikiRoot = "https://github.com/AnthonyLloyd/Fsion/wiki/"

    [<FsionFunction(HelpTopic = wikiRoot + "FQuery", Description = "fsion query")>]
    let FsionGet ([<ExcelArgument(Description="the fsion query")>] query:string) : obj[,] =
        match Text.ofString query with
        | None -> Array2D.create 1 1 ("query missing" :> obj)
        | Some t ->
            match Selector.queryTable selectorContext t with
            | Error t -> Array2D.create 1 1 (Text.toString t :> obj)
            | Ok(attributes, data) ->
                Array2D.mapi (fun i _ i64 ->
                    match Selector.decode selectorContext attributes.[i] i64 with
                    | Ok o -> o
                    | Error e -> "#ERR - " + Text.toString e :> obj
                ) data

    let transactorContext = Unchecked.defaultof<Transactor.Context>

    [<FsionFunction(HelpTopic = wikiRoot + "FCommand", Description = "fsion command")>]
    let FsionSet ([<ExcelArgument(Description="Entity, Attribute, Date, Value table")>] table:obj[,]) =
        let context = selectorContext

        let parseDatum (uri:obj) (attr:obj) (dt:obj) (value:obj) =
            let attribute =
                tryCast attr
                |> Option.bind Text.ofString
                |> Result.ofOption "Attribute not a string"
                |> Result.bind (Selector.attributeId context) 

            Ok (fun e a d v -> e,a,d,v)
            <*> (tryCast uri |> Option.bind Text.ofString
                 |> Result.ofOption "Uri not a string" |> Result.bind (Selector.entity context))
            <*> attribute
            <*> (tryCast dt |> Result.ofOption "Date not valid" |> Result.map Date.ofDateTime)
            <*> (if isNull value then Text.ofString "Value is null" |> Option.get |> Error
                 else match attribute with
                      | Ok a -> Selector.encode context a value
                      | Error _ -> Ok 0L
                )
        
        let parsed =
            Seq.unfold (fun i ->
                let c0 = table.[i,0]
                let c1 = table.[i,1]
                let c2 = table.[i,2]
                let c3 = table.[i,3]
                if isNull c0 && isNull c1 && isNull c2 && isNull c3 then None
                else Some (parseDatum c0 c1 c2 c3, i+1)
            ) 0
            |> Seq.toList
            |> Result.sequence

        match parsed with
        | Error errors ->
            Seq.concat errors
            |> Seq.map Text.toString
            |> String.concat Environment.NewLine
        | Ok datums ->
            {
                TransactionDatum = []
                EntityDatum = datums
                Creates = Selector.newEntity context
                Text = Selector.newText context
                Data = Selector.newData context
            }
            |> Transactor.commit transactorContext
            |> Text.toString