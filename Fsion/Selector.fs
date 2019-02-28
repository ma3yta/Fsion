namespace Fsion

open System
open System.IO
open System.Threading

// TODO: sets, typed entities, decimal

module Selector =

    type Store =
        inherit IDisposable
        abstract member Get : EntityAttribute -> DataSeries voption
        abstract member Ups : EntityAttribute -> (Date * Tx * int64) -> unit
        abstract member GetText : TextId -> Text
        abstract member GetTextId : Text -> TextId
        abstract member TryGetTextId : Text -> TextId voption
        abstract member GetData : DataId -> Data
        abstract member GetDataId : Data -> DataId
        abstract member SnapshotSave : unit -> Result<unit,Text>
        abstract member SnapshotLoad : unit -> Result<unit,Text>
        abstract member CalculateCounts : unit -> Counts * Tx

    let getValue (store:Store) entity attribute date tx =
        match store.Get (EntityAttribute(entity, attribute)) with
        | ValueNone -> 0L
        | ValueSome ds -> DataSeries.getValue date tx ds

    let internal loadTransaction (store:Store) (counts:Counts.Edit) (txn:Transaction) =
        List.iter (store.GetTextId >> ignore) txn.Text
        List.iter (store.GetDataId >> ignore) txn.Data
        let tx = txn.Tx
        List1.iter (fun (ent,att,dat,ven) ->
            store.Ups (EntityAttribute(ent,att)) (dat,tx,ven)
        ) txn.Datum
        Counts.update counts txn

    [<NoComparison;NoEquality>]
    type EntityMapper<'a,'k> = {
        EntityType : EntityType
        KeyAttributes : AttributeId list
        KeyMapping : int64 list -> 'k
        ValueMapping : (Text -> TextId) -> (Data -> DataId) -> 'a -> (AttributeId * Date * int64) list
    }

    let internal createTransaction (mapper:EntityMapper<'a,'k>) (store:Store)
                                   tx counts (date,rows:'a seq) =
        let entityCount = Counts.get mapper.EntityType counts |> int
        let keys = Array.Parallel.init entityCount (fun i ->
            let e = Entity(mapper.EntityType, uint32 i)
            mapper.KeyAttributes
            |> List.map (fun att ->
                getValue store e att date tx
            )
            |> mapper.KeyMapping
        )

        let texts = SetSlim()
        let datas = ListSlim()
        
        let valueMapping =
            let textCount = Counts.getText counts
            let getTextId text =
                match store.TryGetTextId text with
                | ValueSome (TextId i) when i < textCount -> TextId i
                | _ -> textCount + uint32(texts.Add text) |> TextId
            let dataCount = Counts.getData counts
            let getDataId data =
                dataCount + uint32(datas.Add data) |> DataId
            mapper.ValueMapping getTextId getDataId

        Seq.map valueMapping rows
        |> Seq.mapFold (fun count values ->  //TODO: make parallel, getValue bit costs probably
            let key =
                mapper.KeyAttributes
                |> List.map (fun att ->
                    List.fold (fun (ds,ls) (a,d,l) ->
                        if a=att && d>=ds then d,l else ds,ls
                    ) (Date.minValue,0L) values
                    |> snd
                )
                |> mapper.KeyMapping
            match Array.IndexOf(keys, key) with
            | -1 ->
                let e = Entity(mapper.EntityType, uint32 count)
                List.map (fun (a,d,v) -> e,a,d,v) values, count+1
            | i ->
                let e = Entity(mapper.EntityType, uint32 i)
                List.choose (fun (a,d,v) ->
                    if getValue store e a d tx = v then None
                    else Some (e,a,d,v)
                ) values, count
        ) entityCount
        |> fst
        |> List.concat
        |> List1.tryOfList
        |> Option.map (fun datum ->
            {
                Text = texts.ToList()
                Data = datas.ToList()
                Datum = datum
            }
        )

    // Full safe file load save

    let createMemory (snapshotPath:string) =
        let mutable dataSeriesMap = MapSlim()
        let mutable texts = SetSlim()
        let mutable bytes = ListSlim()
        { new Store with
            member __.Get entityAttribute =
                dataSeriesMap.GetOption entityAttribute
            member __.Ups entityAttribute datum =
                let mutable added = false
                let dataSeries = &dataSeriesMap.GetRef(entityAttribute, &added)
                dataSeries <-
                    if added then
                        DataSeries.single datum
                    else
                        DataSeries.append datum dataSeries
            member __.GetText (TextId i) =
                texts.Item(int i)
            member __.GetTextId t =
                texts.Add t |> uint32 |> TextId
            member __.TryGetTextId t =
                texts.Get t
                |> VOption.map (uint32 >> TextId)
            member __.GetData (DataId i) =
                bytes.Item(int i)
            member __.GetDataId bs =
                bytes.Add bs |> uint32 |> DataId
            member __.SnapshotSave() =
                let filename =
                    let goodTimestamp = ""
                    Path.Combine [|snapshotPath;goodTimestamp + ".fsp"|]
                try
                    use fs = File.Create filename
                    StreamSerialize.dataSeriesMapSet fs dataSeriesMap
                    StreamSerialize.textSetSet fs texts
                    StreamSerialize.dataListSet fs bytes
                    Ok ()
                with e -> Error (Text (e.ToString()))
            member __.SnapshotLoad() =
                File.list snapshotPath "*.fsp"
                |> Result.mapError (fun e -> Text (e.ToString()))
                |> Result.map (
                    Array.choose (fun f ->
                        let n = Path.GetFileNameWithoutExtension f
                        match Int32.TryParse n with
                        | true, i -> Some i
                        | false, _ -> None
                    )
                    >> Array.max
                )
                |> Result.map (fun tx ->
                    use fs =
                        let filename = Path.Combine [|snapshotPath;string tx + ".fsp"|]
                        new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read)
                    let ds = StreamSerialize.dataSeriesMapLoad fs
                    let t = StreamSerialize.textSetLoad fs 
                    let b = StreamSerialize.dataListLoad fs
                    dataSeriesMap <- ds
                    texts <- t
                    bytes <- b
                    ()
                )
            member __.CalculateCounts() =
                let mutable txId = 0u
                let edit = Counts.emptyEdit()
                for i = dataSeriesMap.Count downto 0 do
                    let (EntityAttribute(Entity(EntityType et,eid) as ent,_)) =
                        dataSeriesMap.Key i
                    Counts.check edit ent
                    if et = EntityType.Int.transaction && eid > txId then txId <- eid
                Counts.addText edit (uint32 texts.Count)
                Counts.addData edit (uint32 bytes.Count)
                Counts.toCounts edit, Tx txId
          interface IDisposable with
            member __.Dispose() = ()
        }



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