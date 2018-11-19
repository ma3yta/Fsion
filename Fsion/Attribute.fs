namespace Fsion

[<NoComparison;NoEquality>]
type 'a ValueType = internal {
    ToInt: 'a option -> int64
    OfInt: int64 -> 'a option
}

module ValueType =

    let Bool = {
        ToInt =
            function
            | None -> 0L
            | Some i -> if i then 1L else -1L
        OfInt =
            function
            | 0L -> None
            | 1L -> Some true
            | -1L -> Some false
            | i -> failwithf "bool ofint %i" i
    }

    let Int = {
        ToInt =
            function
            | None -> 0L
            | Some i -> if i>0 then int64 i else int64(i-1)
        OfInt =
            function
            | 0L -> None
            | i when i>0L -> int32 i |> Some
            | i -> int32 i + 1 |> Some
    }

    let Int64 = {
        ToInt =
            function
            | None -> 0L
            | Some i -> if i>0L then i else i-1L
        OfInt =
            function
            | 0L -> None
            | i when i>0L -> Some i
            | i -> i + 1L |> Some
    }

    let Uri = {
        ToInt =
            function
            | None -> 0L
            | Some(Uri i) -> unzigzag(i+1u) |> int64
        OfInt =
            function
            | 0L -> None
            | i -> Uri(zigzag(int32 i) - 1u) |> Some
    }

    let Date = {
        ToInt =
            function
            | None -> 0L
            | Some(Date i) -> unzigzag(i+1u) |> int64
        OfInt =
            function
            | 0L -> None
            | i -> Date(zigzag(int32 i) - 1u) |> Some
    }

    let Time = {
        ToInt =
            function
            | None -> 0L
            | Some(Time i) -> if i>0L then i else i-1L
        OfInt =
            function
            | 0L -> None
            | i when i>0L -> Time i |> Some
            | i -> Time(i+1L) |> Some
    }

    let TextId = {
        ToInt =
            function
            | None -> 0L
            | Some(TextId i) -> unzigzag(i+1u) |> int64
        OfInt =
            function
            | 0L -> None
            | i -> TextId(zigzag(int32 i) - 1u) |> Some
    }

    let DataId = {
        ToInt =
            function
            | None -> 0L
            | Some(DataId i) -> unzigzag(i+1u) |> int64
        OfInt =
            function
            | 0L -> None
            | i -> DataId(zigzag(int32 i) - 1u) |> Some
    }

[<NoComparison;NoEquality>]
type Attribute<'a> = {
    Id: AttributeId
    ValueType: ValueType<'a>
}

// TODO: sets, typed entities, decimal

module Attribute =
    let fromId<'a> (db:Database) (AttributeId aid) : Attribute<'a> =
        failwith "hi"
    let fromUri<'a> (db:Database) (Uri uri) : Attribute<'a> =
        failwith "hi"

    let uri : Attribute<Uri> = { Id = AttributeId 0u; ValueType = ValueType.Uri }
    let time : Attribute<Time> = { Id = AttributeId 1u; ValueType = ValueType.Time }

    let get (db:Database) (e:Entity) (a:Attribute<'a>) (d:Date) (tx:Tx) =
        db.DataCache.Get (e,a.Id)
        |> Option.map (DataSeries.get d tx)
        |> Option.bind (fun (d,t,v) ->
            a.ValueType.OfInt v |> Option.map (fun i -> d,t,i))

    let toEntity (AttributeId aid) =
        Entity(EntityType.attribute, aid)

    let validateName (Text t) =
        let inline isLetter c = c>='a' && c<='z'
        let inline isNotLetter c = c<'a' || c>'z'
        let inline isNotDigit c = c>'9' || c<'0'
        let inline isUnderscore c = c='_'
        let rec check i prevUnderscore =
            if i = t.Length then not prevUnderscore
            else
                let c = t.[i]
                if   isNotLetter c
                  && isNotDigit c 
                  && (prevUnderscore || not(isUnderscore c)) then false
                else check (i+1) (isUnderscore c)
        isLetter t.[0] && check 1 false


type SchemaAPI =
    abstract member Attribute : Text -> Result<AttributeId,Text> // "trader" "fund_manager"
    abstract member Entity : Text -> Result<Entity,Text> // "trade/1234" "trade/new1" "party/citibank"
    abstract member Encode : AttributeId -> obj -> Result<int64,Text>
    abstract member Decode : AttributeId -> int64 -> Result<obj,Text>
    abstract member NewEntity : Entity[]
    abstract member NewText : Text[]
    abstract member NewByte : byte[][]

type QueryAPI =
    abstract member Table : Text -> AttributeId[] * int64[,] // "trade" "trade/1234" "trade/1234/quantity" "trade/1234/party/id" "trade/1234/trader/name"

type TransactionAPI =
    abstract member Commit : TransactionData -> Text