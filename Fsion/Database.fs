namespace Fsion

open System
open System.Threading
open System.Diagnostics
open System.Collections.Generic
open System.Threading.Tasks

[<Struct>]
type EntityType =
    | EntityType of int
    static member tx = EntityType 0
    static member entityType = EntityType 1
    static member attribute = EntityType 2

[<Struct>]
type Entity =
    | Entity of EntityType * int

[<CustomEquality;CustomComparison;DebuggerDisplay("{Uri}")>]
type Attribute = {
    Id: int
    Uri: string
    IsSet: bool
    IsString: bool
    Doc: string
} with
    static member uri = {
        Id = 0
        Uri = "uri"
        IsSet = true
        IsString = true
        Doc = "Unique reference for an entity. Can be updated but the previous uris will continue to be other unique references to the entity."
    }
    static member time = {
        Id = 1
        Uri = "time"
        IsSet = false
        IsString = false
        Doc = "Time the transaction was committed to the database."
    }
    member x.Entity =
        Entity(EntityType.attribute, x.Id)
    override x.GetHashCode() =
        x.Id
    override x.Equals o =
        x.Id = (o :?> Attribute).Id
    interface IEquatable<Attribute> with
        member x.Equals(o:Attribute) =
            x.Id = o.Id
    interface IComparable with
        member x.CompareTo o =
            compare x.Id ((o :?> Attribute).Id)

type TextId =
    internal
    | TextId of int

type DataId =
    internal
    | DataId of int

type DataCache =
    inherit IDisposable
    abstract member Get : (Entity * Attribute) -> DataSeries option
    abstract member Set : (Entity * Attribute) -> DataSeries -> unit
    abstract member Ups : (Entity * Attribute) -> Datum -> unit
    abstract member GetTextId : Text -> TextId
    abstract member GetText : TextId -> Text
    abstract member GetDataId : byte[] -> DataId
    abstract member GetData : DataId -> byte[]
    abstract member SnapshotList : unit -> int[]
    abstract member SnapshotSave : int -> unit
    abstract member SnapshotLoad : int -> unit
    abstract member SnapshotDelete : int -> unit

module DataCache =
    let createMemory() =
        let dataSeriesDictionary = Dictionary<Entity * Attribute, DataSeries>()
        let dataSeriesLock = new ReaderWriterLockSlim()
        let stringDictionary = Dictionary StringComparer.Ordinal
        let strings = List()
        let stringLock = new ReaderWriterLockSlim()
        let data = List()
        let dataLock = new ReaderWriterLockSlim()
        { new DataCache with
            member __.Get entityAttribute =
                try
                    dataSeriesLock.EnterReadLock()
                    match dataSeriesDictionary.TryGetValue entityAttribute with
                    | true, ds -> Some ds
                    | false, _ -> None
                finally
                    dataSeriesLock.ExitReadLock()
            member __.Set entityAttribute dataSeries =
                try
                    dataSeriesLock.EnterWriteLock()
                    dataSeriesDictionary.[entityAttribute] <- dataSeries
                finally
                    dataSeriesLock.ExitWriteLock()
            member __.Ups entityAttribute datum =
                try
                    dataSeriesLock.EnterWriteLock()
                    dataSeriesDictionary.[entityAttribute] <-
                        match dataSeriesDictionary.TryGetValue entityAttribute with
                        | true, dataSeries ->
                            DataSeries.append datum dataSeries
                        | false, _ ->
                            DataSeries.single datum
                finally
                    dataSeriesLock.ExitWriteLock()
            member __.GetText (TextId i) =
                stringLock.EnterReadLock()
                try
                    Text strings.[i]
                finally
                    stringLock.ExitReadLock()
            member __.GetTextId (Text s) =
                stringLock.EnterWriteLock()
                try
                    let mutable i = 0
                    if stringDictionary.TryGetValue(s, &i) then TextId i
                    else
                        i <- strings.Count
                        stringDictionary.Add(s,i)
                        strings.Add s
                        TextId i
                finally
                    stringLock.ExitWriteLock()
            member __.GetData (DataId i) =
                dataLock.EnterReadLock()
                try
                    data.[i]
                finally
                    dataLock.ExitReadLock()
            member __.GetDataId bs =
                dataLock.EnterWriteLock()
                try
                    let i = data.Count
                    data.Add bs
                    DataId i
                finally
                    dataLock.ExitWriteLock()
            member __.SnapshotList() : int[] =
                invalidOp "not implemented"
            member __.SnapshotSave txId =
                invalidOp "not implemented"
            member __.SnapshotLoad txId =
                invalidOp "not implemented"
            member __.SnapshotDelete txId =
                invalidOp "not implemented"
          interface IDisposable with
            member __.Dispose() =
                dataSeriesLock.Dispose()
        }

type TxData = {
    Headers: (Attribute * int64) list
    Updates: (Entity * (Attribute * Date * int64) list1) list
    Creates: (EntityType * (Attribute * Date * int64) list1) list
    Strings: string array
}

type TransactionLog =
    abstract member Set : Tx -> TxData -> Result<unit,exn>
    abstract member Get : Tx -> Task<TxData> seq

module TransationLog =
    let createNull() =
        { new TransactionLog with
            member __.Set tx bytes =
                Ok ()
            member __.Get tx =
                invalidOp "null log"
        }
    let createLocal path =
        { new TransactionLog with
            member __.Set (Tx txId) txData =
                let filename = System.IO.Path.Combine(path, (txId.ToString()))
                invalidOp "not implemented"
            member __.Get tx =
                invalidOp "not implemented"
        }


type Database = private {
    DataCache : DataCache
    mutable IndexEntityTypeCount : int array
    mutable IndexEntityTypeAttribute : int [][]
}

module Database =
    let createMemory() =
        {
            DataCache = DataCache.createMemory()
            IndexEntityTypeCount = [|0;0;0|]
            IndexEntityTypeAttribute = [|[||];[||];[||]|]
        }

    [<Literal>]
    let private txEntityTypeIndex = 0
    [<Literal>]
    let private entityTypeEntityTypeIndex = 1

    let private transactionLock = obj()

    let setTransaction (txData: TxData) (time: Time) (db:Database) =
        lock transactionLock (fun () ->
            
            let txId = uint32 db.IndexEntityTypeCount.[txEntityTypeIndex]
            
            let ups (Entity(EntityType etId,_) as entity,attributeList) =
                let mutable attributeArray =
                    db.IndexEntityTypeAttribute.[etId]
                attributeList
                |> List1.toList
                |> Seq.iter (fun (attribute,date,value) ->
                    let value =
                        if attribute.IsString then
                            let (TextId sid) =
                                Text.ofString txData.Strings.[int value]
                                |> db.DataCache.GetTextId
                            int64 sid
                        else value
                    db.DataCache.Ups (entity,attribute)
                        (date,Tx txId,value)
                    if Array.contains attribute.Id attributeArray |> not then
                        Array.Resize(&attributeArray, attributeArray.Length+1)
                        attributeArray.[attributeArray.Length-1] <- attribute.Id
                        db.IndexEntityTypeAttribute.[etId] <- attributeArray
                )
            
            // Updates
            List.iter ups txData.Updates

            let headerList =
                List.map (fun (a,v) -> a,Time.toDate time,v) txData.Headers
                |> List1.init (Attribute.time,Time.toDate time,Time.toInt64 time)

            // Creates
            [EntityType.tx, headerList]
            |> Seq.append txData.Creates
            |> Seq.iter (fun (EntityType entityTypeId,attributeList) ->
                let entityId = db.IndexEntityTypeCount.[entityTypeId]
                let entity = Entity(EntityType entityTypeId, entityId)
                if entityTypeId = entityTypeEntityTypeIndex then
                    let length = db.IndexEntityTypeCount.Length
                    Array.Resize(&db.IndexEntityTypeCount, length+1)
                    Array.Resize(&db.IndexEntityTypeAttribute, length+1)
                    db.IndexEntityTypeAttribute.[length] <- [||]
                ups (entity,attributeList)
                db.IndexEntityTypeCount.[entityTypeId] <- entityId + 1
            )
            
            db.IndexEntityTypeCount.[txEntityTypeIndex] <- int32 txId + 1
        )