namespace Fsion

//type FsionValue =
//    | FsionBool of bool
//    | FsionInt of int
//    | FsionInt64 of int64
//    | FsionUri of Uri
//    | FsionDate of Date
//    | FsionTime of Time
//    | FsionTextId of TextId
//    | FsionDataId of DataId

module FsionValue =
    //let encode (v:FsionValue option) =
    //    match v with
    //    | None -> 0L
    //    | Some(FsionBool i) -> if i then 1L else -1L
    //    | Some(FsionInt i) -> if i>0 then int64 i else int64(i-1)
    //    | Some(FsionInt64 i) -> if i>0L then i else i-1L
    //    | Some(FsionUri(Uri i)) -> unzigzag(i+1u) |> int64
    //    | Some(FsionDate(Date i)) -> unzigzag(i+1u) |> int64
    //    | Some(FsionTime(Time i)) -> if i>0L then i else i-1L
    //    | Some(FsionTextId(TextId i)) -> unzigzag(i+1u) |> int64
    //    | Some(FsionDataId(DataId i)) -> unzigzag(i+1u) |> int64

    let encodeBool i =
        match i with
        | None -> 0L
        | Some i -> if i then 1L else -1L
    
    let decodeBool i =
        match i with
        | 0L -> None
        | 1L -> Some true
        | -1L -> Some false
        | i -> failwithf "bool ofint %i" i

    let encodeInt i =
        match i with
        | None -> 0L
        | Some i -> if i>0 then int64 i else int64(i-1)

    let decodeInt i =
        match i with
        | 0L -> None
        | i when i>0L -> int32 i |> Some
        | i -> int32 i + 1 |> Some

    let encodeInt64 i =
        match i with
        | None -> 0L
        | Some i -> if i>0L then i else i-1L

    let decodeInt64 i =
        match i with
        | 0L -> None
        | i when i>0L -> Some i
        | i -> i + 1L |> Some

    let encodeUri i =
        match i with
        | None -> 0L
        | Some(Uri i) -> unzigzag(i+1u) |> int64
    
    let decodeUri i =
        match i with
        | 0L -> None
        | i -> Uri(zigzag(int32 i) - 1u) |> Some

    let encodeDate i =
        match i with
        | None -> 0L
        | Some(Date i) -> unzigzag(i+1u) |> int64

    let decodeDate i =
        match i with
        | 0L -> None
        | i -> Date(zigzag(int32 i) - 1u) |> Some

    let encodeTime i =
        match i with
        | None -> 0L
        | Some(Time i) -> if i>0L then i else i-1L

    let decodeTime i =
        match i with
        | 0L -> None
        | i when i>0L -> Time i |> Some
        | i -> Time(i+1L) |> Some

    let encodeTextId i =
        match i with
        | None -> 0L
        | Some(TextId i) -> unzigzag(i+1u) |> int64

    let decodeTextId i =
        match i with
        | 0L -> None
        | i -> TextId(zigzag(int32 i) - 1u) |> Some

    let encodeDataId i =
        match i with
        | None -> 0L
        | Some(DataId i) -> unzigzag(i+1u) |> int64

    let decodeDataId i =
        match i with
        | 0L -> None
        | i -> DataId(zigzag(int32 i) - 1u) |> Some


//[<NoComparison;NoEquality>]
//type Attribute<'a> = {
//    Id: AttributeId
//    ValueType: ValueType<'a>
//}

// TODO: sets, typed entities, decimal

module Selector =

    [<NoComparison>]
    type Context =
        private
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