namespace Fsion.Excel

open System
open ExcelDna.Integration
open Fsion

type private FsionFunction() =
    inherit ExcelFunctionAttribute(
        Category = "Fsion",
        IsExceptionSafe = true,
        IsThreadSafe = true
    )

module API =
    [<Literal>]
    let private unset = "#Unset"

    let private encode (t:FsionType) (o:obj) =
        let ofObj (o:obj) =
            let comp = StringComparison.OrdinalIgnoreCase
            match o with
            | :? string as s when unset.Equals(s,comp) -> Ok None
            | o ->
                tryCast o
                |> Result.ofOption "not a valid %s: %A" (Text.toString t.Name) o
                |> Result.map Some
        match t with
        | TypeType ->
            tryCast o
            |> Option.bind Text.ofString
            |> Result.ofOption "type not text"
            |> Result.bind (function
                | IsText unset -> Ok None
                | t -> FsionType.Parse t |> Result.map Some
            )
            |> Result.map FsionValue.encodeType
        | TypeBool -> ofObj o |> Result.map FsionValue.encodeBool
        | TypeInt -> ofObj o |> Result.map FsionValue.encodeInt
        | TypeInt64 -> ofObj o |> Result.map FsionValue.encodeInt64
        | TypeUri -> ofObj o |> Result.map FsionValue.encodeUri
        | TypeDate ->
            ofObj o
            |> Result.map (Option.map Date.ofDateTime >> FsionValue.encodeDate)
        | TypeTime ->
            ofObj o
            |> Result.map (Option.map Time.ofDateTime >> FsionValue.encodeTime)
        | TypeTextId ->  ofObj o |> Result.map FsionValue.encodeTextId
        | TypeDataId -> ofObj o |> Result.map FsionValue.encodeDataId

    let private decode (t:FsionType) (i64:int64) =
        let toObj = function Some a -> a :> obj | None -> unset :> obj
        match t with
        | TypeType ->
            FsionValue.decodeType i64
            |> Option.map (fun i -> Text.toString i.Name)
            |> toObj
        | TypeBool -> FsionValue.decodeBool i64 |> toObj
        | TypeInt -> FsionValue.decodeInt i64 |> toObj
        | TypeInt64 -> FsionValue.decodeInt64 i64 |> toObj
        | TypeUri -> FsionValue.decodeUri i64 |> toObj
        | TypeDate -> FsionValue.decodeDate i64 |> toObj
        | TypeTime -> FsionValue.decodeTime i64 |> toObj
        | TypeTextId -> FsionValue.decodeTextId i64 |> toObj
        | TypeDataId -> FsionValue.decodeTextId i64 |> toObj

    let private database =
        Database.createMemory "C:/temp/FsionTest"

    let private selectorContext =
        Selector.localContext database
    
    [<Literal>]
    let private wikiRoot = "https://github.com/AnthonyLloyd/Fsion/wiki/"

    [<FsionFunction(HelpTopic = wikiRoot + "FsionGet", Description = "fsion get")>]
    let FsionGet ([<ExcelArgument(Description="the fsion query")>] query:string) : obj[,] =
        match Text.ofString query with
        | None -> Array2D.create 1 1 ("query missing" :> obj)
        | Some t ->
            match Selector.queryTable selectorContext t with
            | Error t -> Array2D.create 1 1 (Text.toString t :> obj)
            | Ok (attributes, data) ->
                Array2D.mapi (fun i _ i64 ->
                    let attributeType = Selector.attributeType selectorContext attributes.[i]
                    decode attributeType i64
                ) data

    let transactorContext =
        Transactor.localContext database

    [<FsionFunction(HelpTopic = wikiRoot + "FsionSet", Description = "fsion set")>]
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
            <*> (if isNull value then Text "Value is null" |> Error
                 else match attribute with
                      | Ok a ->
                        let attributeType = Selector.attributeType context a
                        encode attributeType value
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