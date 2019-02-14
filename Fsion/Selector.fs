namespace Fsion

open System
open System.IO
open System.Threading
open Fsion

// TODO: sets, typed entities, decimal

module Selector =

    type Store =
        inherit IDisposable
        abstract member Get : EntityAttribute -> DataSeries voption
        abstract member Set : EntityAttribute -> DataSeries -> unit
        abstract member Ups : EntityAttribute -> (Date * Tx * int64) -> unit
        abstract member TryGetTextId : Text -> TextId voption
        abstract member GetTextId : Text -> TextId
        abstract member GetText : TextId -> Text
        abstract member GetDataId : byte[] -> DataId
        abstract member GetData : DataId -> byte[]
        abstract member SnapshotList : unit -> Result<int[],exn>
        abstract member SnapshotSave : int -> Result<unit,exn>
        abstract member SnapshotLoad : int -> Result<unit,exn>
        abstract member SnapshotDelete : int -> Result<unit,exn>

    [<NoComparison>]
    type Context =
        private
        | Local of Store
        | Create of Store * ResizeArray<Entity> * ResizeArray<Text> * ResizeArray<byte[]>

    let localContext database =
        Local database

    let toEntity (AttributeId aid) =
        Entity(EntityType.attribute, aid)

    let entityTypeUriLookup (i:TextId) : EntityType option = failwith "todo"
    let entityUriLookup (i:TextId) : uint32 option = failwith "todo"
    let attributeUriLookup (i:TextId) : uint32 option = failwith "todo"

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
                    | ValueNone -> Text("entity type uri not recognised: "+uri.Substring(0,i)) |> Error
                    | ValueSome textId ->
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
                    | ValueNone -> Text("entity uri not recognised: "+uri.Substring(i+1,uri.Length)) |> Error
                    | ValueSome textId ->
                        match entityUriLookup textId with
                        | None -> Text("entity uri not recognised on entity type: "+uri) |> Error
                        | Some i -> Ok i
                | UriInvalid -> Text("entity uri is not a valid uri: "+uri.Substring(i+1,uri.Length)) |> Error

            match entityType, entityId with
            | Ok et, Ok i -> Entity(et, i) |> Ok
            | Ok _, Error e -> Error e
            | Error e, Ok _ -> Error e
            | Error e1, Error e2 -> Error (e1 + ". " + e2)

    let attributeId (cx:Context) (text:Text) = // "trader" "fund_manager"
        match text,0,Text.length text with
        | UriInt i -> AttributeId i |> Ok
        | UriNew _ -> "attribute cannot be new: " + text |> Error
        | UriUri ->
            let db = match cx with | Local i -> i | Create (i,_,_,_) -> i
            match db.TryGetTextId text with
            | ValueNone -> "attribute uri not recognised: " + text |> Error
            | ValueSome textId ->
                match attributeUriLookup textId with
                | None -> "uri not recognised as an attribute: " + text |> Error
                | Some i -> AttributeId i |> Ok
        | UriInvalid -> "attribute is not a valid uri: " + text |> Error

    
    //let get (db:Database) (e:Entity) (a:Attribute<'a>) (d:Date) (tx:Tx) =
    //    db.Get (e,a.Id)
    //    |> Option.map (DataSeries.get d tx)
    //    |> Option.bind (fun (d,t,v) ->
    //        a.ValueType.OfInt v |> Option.map (fun i -> d,t,i))

    let attributeType (cx:Context) (attribute:AttributeId) = // TODO: assumes type exists and is same over time
        let db = match cx with | Local i -> i | Create (i,_,_,_) -> i
        db.Get(EntityAttribute(toEntity attribute, AttributeId.attribute_type))
        |> VOption.map (DataSeries.get Date.maxValue Tx.maxValue)
        |> VOption.get
        |> trd
        |> FsionValue.decodeType
        |> Option.get

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


    let createMemory (snapshotPath:string) =
        let mutable dataSeriesMap = MapSlim()
        let mutable texts = SetSlim()
        let mutable bytes = ListSlim()
        { new Store with
            member __.Get entityAttribute =
                dataSeriesMap.GetOption entityAttribute
            member __.Set entityAttribute dataSeries =
                Monitor.Enter dataSeriesMap
                try
                    dataSeriesMap.Set(entityAttribute, dataSeries)
                finally
                    Monitor.Exit dataSeriesMap
            member __.Ups entityAttribute datum =
                Monitor.Enter dataSeriesMap
                try
                    let mutable added = false
                    let dataSeries = &dataSeriesMap.GetRef(entityAttribute, &added)
                    dataSeries <-
                        if added then
                            DataSeries.single datum
                        else
                            DataSeries.append datum dataSeries
                finally
                    Monitor.Exit dataSeriesMap
            member __.GetText (TextId i) =
                texts.Item(int i)
            member __.TryGetTextId t =
                texts.Get t
                |> VOption.map (uint32 >> TextId)
            member __.GetTextId t =
                Monitor.Enter texts
                try
                    texts.Add t |> uint32 |> TextId
                finally
                    Monitor.Exit texts
            member __.GetData (DataId i) =
                bytes.Item(int i)
            member __.GetDataId bs =
                Monitor.Enter bytes
                try
                    bytes.Add bs |> uint32 |> DataId
                finally
                    Monitor.Exit bytes
            member __.SnapshotList() =
                File.list snapshotPath "*.fsp"
                |> Result.map (Array.choose (fun f ->
                        let n = Path.GetFileNameWithoutExtension f
                        match Int32.TryParse n with
                        | true, i -> Some i
                        | false, _ -> None
                    )
                )
            member __.SnapshotSave txId =
                try
                    use fs =
                        Path.Combine [|snapshotPath;txId.ToString()+".fsp"|]
                        |> File.Create
                    StreamSerialize.dataSeriesMapSet fs dataSeriesMap
                    StreamSerialize.textSetSet fs texts
                    StreamSerialize.byteListSet fs bytes
                    Ok ()
                with e -> Error e
            member __.SnapshotLoad txId =
                use fs =
                    let filename = Path.Combine [|snapshotPath;txId.ToString()+".fsp"|]
                    new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read)
                dataSeriesMap <- StreamSerialize.dataSeriesMapLoad fs
                texts <- StreamSerialize.textSetLoad fs 
                bytes <- StreamSerialize.byteListLoad fs
                Ok ()
            member __.SnapshotDelete txId =
                try
                    Path.Combine [|snapshotPath;txId.ToString()+".fsp"|]
                    |> File.Delete |> Ok
                with e -> Error e
          interface IDisposable with
            member __.Dispose() = ()
        }