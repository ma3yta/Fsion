namespace Fsion

type FsionType =
    | FsionBool
    | FsionInt
    | FsionInt64
    | FsionUri
    | FsionDate
    | FsionTime
    | FsionTextId
    | FsionDataId

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


[<NoComparison;NoEquality>]
type Attribute<'a> = {
    Id: AttributeId
    ValueType: FsionType
    IsSet: bool
}

module Attribute =
    let uri : Attribute<Uri> = { Id = AttributeId.uri; ValueType = FsionUri; IsSet = true }
    let time : Attribute<Time> = { Id = AttributeId.time; ValueType = FsionTime; IsSet = false }
    let attribute_type : Attribute<Time> = { Id = AttributeId.attribute_type; ValueType = FsionTime; IsSet = false }
    let attribute_isset : Attribute<bool> = { Id = AttributeId.attribute_isset; ValueType = FsionBool; IsSet = false }

// TODO: sets, typed entities, decimal

module Selector =

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

    let internal validateUri (Text t) i j =
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

    let internal validateInt (Text t) i j =
        let inline isNotDigit c = c>'9' || c<'0'
        let rec check i =
            if i = j then true
            else
                if isNotDigit t.[i] then false
                else check (i+1)
        isNotDigit t.[i] |> not && check (i+1)

    let internal toInt (Text t) i j =
        let rec calc n i =
            if i=j then n
            else calc (10u*n+(uint32 t.[i] - 48u)) (i+1)
        calc 0u i

    let internal (|UriInt|UriNew|UriUri|UriInvalid|) (Text t,i,j) =
        if validateInt (Text t) i j then
            let n = toInt (Text t) i j
            UriInt n
        elif t.[i]='n' && i+3<=j && t.[i+1]='e' && t.[i+2]='w'
          && validateInt (Text t) (i+3) j then
            let n = toInt (Text t) (i+3) j
            UriNew n
        elif validateUri (Text t) i j then UriUri
        else UriInvalid

    let entityTypeUriLookup (i:TextId) : EntityType option = failwith "todo"
    let entityUriLookup (i:TextId) : uint32 option = failwith "todo"

    let entity (cx:Context) (Text uri) : Result<Entity,Text> = // "4/123" "trade/1234" "trade/new1" "party/citibank"
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
                | UriNew i ->
                    // TODO: Add to new entity
                    Ok i
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
    
    let attributeId (cx:Context) (uri:Text) : Result<AttributeId,Text> = // "trader" "fund_manager"
        failwith "attributeId"

    let encode (cx:Context) (attribute:AttributeId) (o:obj) : Result<int64,Text> =
        failwith "encode"

    let decode (cx:Context) (sttribute:AttributeId) (int64) : Result<obj,Text> =
        failwith "decode"

    let newEntity (cx:Context) =
        match cx with
        | Local _ -> Array.empty
        | Create (_,e,_,_) -> e.ToArray()

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