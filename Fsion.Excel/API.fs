namespace Fsion.Excel

open System
open Fsion
open ExcelDna.Integration

type EntityNew =
    | Existing of Entity
    | New of Entity

type EncodedValue =
    | EncodedInt of int64
    | EncodedText of Text
    | EncodedByte of byte[]

type DecodedValue =
    | DecodedObj of obj
    | DecodedText of int
    | DecodedByte of int

type SelectAPI =
    abstract member Attribute : Text -> Result<AttributeId,Text>
    abstract member Entity : Text -> Result<EntityNew,Text> // "trade/1234" "trade/new1"
    abstract member Encode : AttributeId -> obj -> Result<EncodedValue,Text>
    abstract member Decode : AttributeId -> int64 -> Result<DecodedValue,Text>

type QueryAPI =
    abstract member Table : Text -> AttributeId[] * int64[,] * Text[] * byte[][]

type TransactionAPI =
    abstract member Set : TransactionData -> Text

//type ObervableAPI =
//    abstract member Attribute : string -> AttributeId option

type private FsionFunction() =
    inherit ExcelFunctionAttribute(
        Category = "Fsion",
        IsExceptionSafe = true,
        IsThreadSafe = true
    )

module API =

    let selectAPI = Unchecked.defaultof<SelectAPI>
    let queryAPI = Unchecked.defaultof<QueryAPI>

    [<Literal>]
    let private wikiRoot = "https://github.com/AnthonyLloyd/Fsion/wiki/"

    [<FsionFunction(HelpTopic = wikiRoot + "FQuery", Description = "fsion query")>]
    let FsionGet ([<ExcelArgument(Description="the fsion query")>] query:string) : obj[,] =
        match Text.ofString query with
        | None -> Array2D.create 1 1 ("query missing" :> obj)
        | Some t ->
            let attributes, data, texts, bytes = queryAPI.Table t
            Array2D.mapi (fun i _ i64 ->
                match selectAPI.Decode attributes.[i] i64 with
                | Ok(DecodedObj o) -> o
                | Ok(DecodedText i) -> texts.[i] |> Text.toString :> obj
                | Ok(DecodedByte i) -> bytes.[i] :> obj
                | Error e -> "#ERR - " + Text.toString e :> obj
            ) data

    let transactionAPI = Unchecked.defaultof<TransactionAPI>

    [<FsionFunction(HelpTopic = wikiRoot + "FCommand", Description = "fsion command")>]
    let FsionSet ([<ExcelArgument(Description="Entity, Attribute, Date, Value table")>] table:obj[,]) =
        
        let creates = new ResizeArray<_>()
        let entity = memoize (fun i ->
            selectAPI.Entity i
            |> Result.map (function
                | Existing e -> e
                | New e -> creates.Add e; e
            )
        )
        let attribute = memoize selectAPI.Attribute
        
        let texts = new ResizeArray<_>()
        let bytes = new ResizeArray<_>()

        let value (a:AttributeId) (o:obj) =
            selectAPI.Encode a o
            |> Result.map (
                function
                | EncodedInt i -> i
                | EncodedText t ->
                    texts.Add t
                    int64(texts.Count-1)
                | EncodedByte b ->
                    bytes.Add b
                    int64(texts.Count-1)
            )

        let parseDatum c0 c1 c2 c3 : Result<Datum,_> =
            let attribute = tryCast c1 |> Result.ofOption "Attribute not text" |> Result.bind attribute
            Ok (fun e a d v -> e,a,d,v)
            <*> (tryCast c0 |> Result.ofOption "Uri not text" |> Result.bind entity)
            <*> attribute
            <*> (tryCast c2 |> Result.ofOption "Date not valid" |> Result.map Date.ofDateTime)
            <*> (if isNull c3 then Text.ofString "Value is null" |> Option.get |> Error
                 else match attribute with
                      | Ok a -> value a c3
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
                Creates = creates.ToArray()
                Text = texts.ToArray()
                Data = bytes.ToArray()
            }
            |> transactionAPI.Set
            |> Text.toString