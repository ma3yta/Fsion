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

//[<NoComparison;NoEquality>]
//type Attribute<'a> = {
//    Id: AttributeId
//    ValueType: ValueType<'a>
//}

// TODO: sets, typed entities, decimal

module Selector =

    [<NoComparison>]
    type Context =
        | Local of Database

    //let uri : Attribute<Uri> = { Id = AttributeId 0u; ValueType = ValueType.Uri }
    //let time : Attribute<Time> = { Id = AttributeId 1u; ValueType = ValueType.Time }

    //let get (db:Database) (e:Entity) (a:Attribute<'a>) (d:Date) (tx:Tx) =
    //    db.Get (e,a.Id)
    //    |> Option.map (DataSeries.get d tx)
    //    |> Option.bind (fun (d,t,v) ->
    //        a.ValueType.OfInt v |> Option.map (fun i -> d,t,i))

    let toEntity (AttributeId aid) =
        Entity(EntityType.attribute, aid)

    let internal validateName (Text t) =
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

    let attributeId (cx:Context) (uri:Text) : Result<AttributeId,Text> = // "trader" "fund_manager"
        failwith "attributeId"

    let entity (cx:Context) (uri:Text) : Result<Entity,Text> = // "trade/1234" "trade/new1" "party/citibank"
        failwith "entity"

    let encode (cx:Context) (attribute:AttributeId) (o:obj) : Result<int64,Text> =
        failwith "encode"

    let decode (cx:Context) (sttribute:AttributeId) (int64) : Result<obj,Text> =
        failwith "decode"

    let newEntity (cx:Context) : Entity[] =
        failwith "newEntity"

    let newText (cx:Context) : Text[] =
        failwith "newText"

    let newData (cx:Context) : byte[][] =
        failwith "newData"

    let queryTable (cx:Context) (query:Text) : AttributeId[] * int64[,] = // "trade" "trade/1234" "trade/1234/quantity" "trade/1234/party/id" "trade/1234/trader/name"
        failwith "query"