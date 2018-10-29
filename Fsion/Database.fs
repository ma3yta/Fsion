namespace Fsion

open System
open System.Threading
open System.Collections.Generic
open System.Threading.Tasks
open Fsion

type DataCache =
    inherit IDisposable
    abstract member Get : (Entity * Attribute) -> DataSeries option
    abstract member Set : (Entity * Attribute) -> DataSeries -> unit
    abstract member Ups : (Entity * Attribute) -> Datum -> unit
    abstract member GetTextId : Text -> TextId
    abstract member GetText : TextId -> Text
    abstract member GetDataId : byte[] -> DataId
    abstract member GetData : DataId -> byte[]
    abstract member SnapshotList : unit -> Result<int[],exn>
    abstract member SnapshotSave : int -> Result<unit,exn>
    abstract member SnapshotLoad : int -> Result<unit,exn>
    abstract member SnapshotDelete : int -> Result<unit,exn>

module DataCache =
    open System.IO

    let createMemory (snapshotPath:string) =
        let dataSeriesDictionary = Dictionary<Entity * Attribute, DataSeries>()
        let dataSeriesLock = new ReaderWriterLockSlim()
        let stringDictionary = Dictionary StringComparer.Ordinal
        let texts = ResizeArray()
        let textLock = new ReaderWriterLockSlim()
        let bytes = ResizeArray()
        let byteLock = new ReaderWriterLockSlim()
        { new DataCache with
            member __.Get entityAttribute =
                dataSeriesLock.EnterReadLock()
                try
                    match dataSeriesDictionary.TryGetValue entityAttribute with
                    | true, ds -> Some ds
                    | false, _ -> None
                finally
                    dataSeriesLock.ExitReadLock()
            member __.Set entityAttribute dataSeries =
                dataSeriesLock.EnterWriteLock()
                try
                    dataSeriesDictionary.[entityAttribute] <- dataSeries
                finally
                    dataSeriesLock.ExitWriteLock()
            member __.Ups entityAttribute datum =
                dataSeriesLock.EnterWriteLock()
                try
                    dataSeriesDictionary.[entityAttribute] <-
                        match dataSeriesDictionary.TryGetValue entityAttribute with
                        | true, dataSeries ->
                            DataSeries.append datum dataSeries
                        | false, _ ->
                            DataSeries.single datum
                finally
                    dataSeriesLock.ExitWriteLock()
            member __.GetText (TextId i) =
                textLock.EnterReadLock()
                try
                    texts.[i]
                finally
                    textLock.ExitReadLock()
            member __.GetTextId (Text s) =
                textLock.EnterWriteLock()
                try
                    let mutable i = 0
                    if stringDictionary.TryGetValue(s, &i) then TextId i
                    else
                        i <- texts.Count
                        stringDictionary.Add(s,i)
                        texts.Add (Text s)
                        TextId i
                finally
                    textLock.ExitWriteLock()
            member __.GetData (DataId i) =
                byteLock.EnterReadLock()
                try
                    bytes.[i]
                finally
                    byteLock.ExitReadLock()
            member __.GetDataId bs =
                byteLock.EnterWriteLock()
                try
                    let i = bytes.Count
                    bytes.Add bs
                    DataId i
                finally
                    byteLock.ExitWriteLock()
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
                dataSeriesLock.EnterReadLock()
                textLock.EnterReadLock()
                byteLock.EnterReadLock()
                try
                    try
                        use fs =
                            Path.Combine [|snapshotPath;txId.ToString()+".fsp"|]
                            |> File.Create
                        StreamSerialize.dataSeriesDictionarySet dataSeriesDictionary fs
                        StreamSerialize.textListSet texts fs
                        StreamSerialize.byteListSet bytes fs
                        Ok ()
                    with e -> Error e
                finally
                    byteLock.ExitReadLock()
                    textLock.ExitReadLock()
                    dataSeriesLock.ExitReadLock()
            member __.SnapshotLoad txId =
                dataSeriesLock.EnterWriteLock()
                textLock.EnterWriteLock()
                byteLock.EnterWriteLock()
                try
                    use fs =
                        let filename = Path.Combine [|snapshotPath;txId.ToString()+".fsp"|]
                        new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read)
                    StreamSerialize.dataSeriesDictionaryLoad dataSeriesDictionary fs
                    StreamSerialize.textListLoad fs texts
                    StreamSerialize.byteListLoad fs bytes
                    Ok ()
                finally
                    byteLock.ExitWriteLock()
                    textLock.ExitWriteLock()
                    dataSeriesLock.ExitWriteLock()
            member __.SnapshotDelete txId =
                try
                    Path.Combine [|snapshotPath;txId.ToString()+".fsp"|]
                    |> File.Delete |> Ok
                with e -> Error e
          interface IDisposable with
            member __.Dispose() =
                dataSeriesLock.Dispose()
        }

type TransactionData = {
    Headers: (Attribute * int64) list
    Creates: (Entity * Attribute * Date * int64) list
    Updates: (Entity * Attribute * Date * int64) list
    Strings: string[]
    Data: byte[]
}

type TransactionLog =
    abstract member Set : Tx -> TransactionData -> Result<unit,exn>
    abstract member Get : Tx -> Task<TransactionData> seq

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
    mutable IndexEntityTypeCount : uint32 array
    mutable IndexEntityTypeAttribute : uint32 [][]
}

module Database =
    let createMemory snapshotPath =
        {
            DataCache = DataCache.createMemory snapshotPath
            IndexEntityTypeCount = [|0u;0u;0u|]
            IndexEntityTypeAttribute = [|[||];[||];[||]|]
        }

    [<Literal>]
    let private txEntityTypeIndex = 0
    [<Literal>]
    let private entityTypeEntityTypeIndex = 1

    let private transactionLock = obj()

    let setTransaction (txData: TransactionData) (time: Time) (db:Database) =
        lock transactionLock (fun () ->
            
            let txId = uint32 db.IndexEntityTypeCount.[txEntityTypeIndex]
            
            let ups (Entity(EntityType etId,_) as entity,Attribute attributeId,date,value) =
                let mutable attributeArray =
                    &db.IndexEntityTypeAttribute.[int etId]
                db.DataCache.Ups (entity,Attribute attributeId)
                    (date,Tx txId,value)
                if Array.contains attributeId attributeArray |> not then
                    Array.Resize(&attributeArray, attributeArray.Length+1)
                    attributeArray.[attributeArray.Length-1] <- attributeId
                    db.IndexEntityTypeAttribute.[int etId] <- attributeArray
            
            let date = Time.toDate time
            let txEntity = Entity(EntityType.tx, txId)

            txData.Headers
            |> Seq.append [Attribute.time, Time.toInt64 time]
            |> Seq.map (fun (a,v) -> txEntity,a,date,v)
            |> Seq.iter ups

            Seq.iter ups txData.Creates
            Seq.iter ups txData.Updates

            db.IndexEntityTypeCount.[txEntityTypeIndex] <- txId + 1u
        )