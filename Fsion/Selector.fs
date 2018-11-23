namespace Fsion

open System

type ValueType =
    | FsionValueType
    | FsionBool
    | FsionInt
    | FsionInt64
    | FsionUri
    | FsionDate
    | FsionTime
    | FsionTextId
    | FsionDataId
    member i.Encode() =
        match i with
        | FsionValueType -> 1L
        | FsionBool -> 2L
        | FsionInt -> 3L
        | FsionInt64 -> 4L
        | FsionUri -> 5L
        | FsionDate -> 6L
        | FsionTime -> 7L
        | FsionTextId -> 8L
        | FsionDataId -> 9L
    static member Decode i =
        match i with
        | 1L -> FsionValueType
        | 2L -> FsionBool
        | 3L -> FsionInt
        | 4L -> FsionInt64
        | 5L -> FsionUri
        | 6L -> FsionDate
        | 7L -> FsionTime
        | 8L -> FsionTextId
        | 9L -> FsionDataId
        | _ -> FsionInt

type FsionValue =
    | FsionValueType of ValueType
    | FsionValueBool of bool
    | FsionValueInt of int
    | FsionValueInt64 of int64
    | FsionValueUri of Fsion.Uri
    | FsionValueDate of Date
    | FsionValueTime of Time
    | FsionValueTextId of TextId
    | FsionValueDataId of DataId

module FsionValue =
    let encode (v:FsionValue option) =
        match v with
        | None -> 0L
        | Some(FsionValueType i) -> i.Encode()
        | Some(FsionValueBool i) -> if i then 1L else -1L
        | Some(FsionValueInt i) -> if i>0 then int64 i else int64(i-1)
        | Some(FsionValueInt64 i) -> if i>0L then i else i-1L
        | Some(FsionValueUri(Uri i)) -> unzigzag(i+1u) |> int64
        | Some(FsionValueDate(Date i)) -> unzigzag(i+1u) |> int64
        | Some(FsionValueTime(Time i)) -> if i>0L then i else i-1L
        | Some(FsionValueTextId(TextId i)) -> unzigzag(i+1u) |> int64
        | Some(FsionValueDataId(DataId i)) -> unzigzag(i+1u) |> int64

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
        | i -> Fsion.Uri(zigzag(int32 i) - 1u) |> Some

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


[<NoComparison;NoEquality>]
type Attribute<'a> = {
    Id: AttributeId
    ValueType: ValueType
    IsSet: bool
}

module Attribute =
    let uri : Attribute<Uri> = { Id = AttributeId.uri; ValueType = FsionUri; IsSet = true }
    let time : Attribute<Time> = { Id = AttributeId.time; ValueType = FsionTime; IsSet = false }
    let attribute_type : Attribute<Time> = { Id = AttributeId.attribute_type; ValueType = FsionTime; IsSet = false }
    let attribute_isset : Attribute<bool> = { Id = AttributeId.attribute_isset; ValueType = FsionBool; IsSet = false }

// TODO: sets, typed entities, decimal

module Selector =

    let internal (|UriInt|UriNew|UriUri|UriInvalid|) (Text t,i,j) =
        
        let inline validateUri i j =
            let inline isLetter c = c>='a' && c<='z'
            let inline isNotLetter c = c<'a' || c>'z'
            let inline isNotDigit c = c>'9' || c<'0'
            let inline isUnderscore c = c='_'
            let rec check i prevUnderscore =
                if i = j then not prevUnderscore
                else
                    let c = t.[i]
                    if   isNotLetter c
                      && isNotDigit c 
                      && (prevUnderscore || not(isUnderscore c)) then false
                    else check (i+1) (isUnderscore c)
            isLetter t.[i] && check (i+1) false

        let inline validateInt i j =
            let inline isNotDigit c = c>'9' || c<'0'
            let rec check i =
                if i = j then true
                else
                    if isNotDigit t.[i] then false
                    else check (i+1)
            isNotDigit t.[i] |> not && check (i+1)

        let inline toInt i j =
            let rec calc n i =
                if i=j then n
                else calc (10u*n+(uint32 t.[i] - 48u)) (i+1)
            calc 0u i

        if validateInt i j then
            let n = toInt i j
            UriInt n
        elif t.[i]='n' && i+3<=j && t.[i+1]='e' && t.[i+2]='w'
          && validateInt (i+3) j then
            let n = toInt (i+3) j
            UriNew n
        elif validateUri i j then UriUri
        else UriInvalid

    [<NoComparison>]
    type Context =
        private
        | Local of Database
        | Create of Database * ResizeArray<Entity> * ResizeArray<Text> * ResizeArray<byte[]>

    //let get (db:Database) (e:Entity) (a:Attribute<'a>) (d:Date) (tx:Tx) =
    //    db.Get (e,a.Id)
    //    |> Option.map (DataSeries.get d tx)
    //    |> Option.bind (fun (d,t,v) ->
    //        a.ValueType.OfInt v |> Option.map (fun i -> d,t,i))

    let toEntity (AttributeId aid) =
        Entity(EntityType.attribute, aid)

    let entityTypeUriLookup (i:TextId) : EntityType option = failwith "todo"
    let entityUriLookup (i:TextId) : uint32 option = failwith "todo"

    let entity (cx:Context) (Text uri) = // "4/123" "trade/1234" "trade/new1" "party/citibank"
        match uri.IndexOf('/') with
        | -1 -> Text("entity not a two part entity type/id or type/uri: "+uri) |> Error
        | i ->
            let entityType =
                match Text uri,0,i with
                | UriInt i -> EntityType i |> Ok
                | UriNew _ -> Text("entity type cannot be new: "+uri.Substring(0,i)) |> Error
                | UriUri ->
                    let db = match cx with | Local i -> i | Create (i,_,_,_) -> i
                    match db.TryGetTextId (uri.Substring(0,i) |> Text) with
                    | None -> Text("entity type uri not recognised: "+uri.Substring(0,i)) |> Error
                    | Some textId ->
                        match entityTypeUriLookup textId with
                        | None -> Text("entity type uri not an entity type: "+uri.Substring(0,i)) |> Error
                        | Some et -> Ok et
                | UriInvalid -> Text("entity type is not a valid uri: "+uri.Substring(0,i)) |> Error

            let entityId =
                match Text uri,i+1,uri.Length with
                | UriInt i -> Ok i
                | UriNew ui ->
                    match cx with
                    | Local _ -> Text("Selector.Context needs to be Create for: "+uri.Substring(i+1,uri.Length)) |> Error
                    | Create (_,es,_,_) -> 
                        match entityType with
                        | Ok et ->
                            let j =
                                match es.IndexOf(Entity(et,ui)) with
                                | -1 ->
                                    es.Add(Entity(et,ui))
                                    es.Count-1
                                | j -> j
                            (UInt32.MaxValue - uint32 j) |> Ok
                        | Error _ -> Ok ui
                | UriUri ->
                    let db = match cx with | Local i -> i | Create (i,_,_,_) -> i
                    match db.TryGetTextId (uri.Substring(0,i) |> Text) with
                    | None -> Text("entity uri not recognised: "+uri.Substring(i+1,uri.Length)) |> Error
                    | Some textId ->
                        match entityUriLookup textId with
                        | None -> Text("entity uri not recognised on entity type: "+uri) |> Error
                        | Some i -> Ok i
                | UriInvalid -> Text("entity uri is not a valid uri: "+uri.Substring(i+1,uri.Length)) |> Error

            match entityType, entityId with
            | Ok et, Ok i -> Entity(et, i) |> Ok
            | Ok _, Error e -> Error e
            | Error e, Ok _ -> Error e
            | Error e1, Error e2 -> Error (e1 + ". " + e2)

    let attributeUriLookup (i:TextId) : uint32 option = failwith "todo"
        
    let attributeId (cx:Context) (text:Text) = // "trader" "fund_manager"
        match text,0,Text.length text with
        | UriInt i -> AttributeId i |> Ok
        | UriNew _ -> "attribute cannot be new: " + text |> Error
        | UriUri ->
            let db = match cx with | Local i -> i | Create (i,_,_,_) -> i
            match db.TryGetTextId text with
            | None -> "attribute uri not recognised: " + text |> Error
            | Some textId ->
                match attributeUriLookup textId with
                | None -> "uri not recognised as an attribute: " + text |> Error
                | Some i -> AttributeId i |> Ok
        | UriInvalid -> "attribute is not a valid uri: " + text |> Error

    let encode (cx:Context) (attribute:AttributeId) (o:FsionValue option) : Result<int64,Text> =
        failwith "encode"

    let decode (cx:Context) (attribute:AttributeId) (i:int64) : Result<FsionValue,Text> =
        failwith "decode"

    let newEntity (cx:Context) =
        match cx with
        | Local _ -> Array.empty
        | Create (_,e,_,_) ->
            Seq.mapi (fun i (Entity(et,_)) ->
                Entity(et, UInt32.MaxValue - uint32 i)
            ) e
            |> Seq.toArray

    let newText (cx:Context) =
        match cx with
        | Local _ -> Array.empty
        | Create (_,_,t,_) -> t.ToArray()

    let newData (cx:Context) =
        match cx with
        | Local _ -> Array.empty
        | Create (_,_,_,d) -> d.ToArray()

    let queryTable (cx:Context) (query:Text) : Result<AttributeId[] * int64[,],Text> = // "trade" "trade/1234" "trade/1234/quantity" "trade/1234/party/id" "trade/1234/trader/name"
        failwith "query"