namespace Fsion

open System
open System.IO
open System.Threading
open System.Collections.Generic
open Fsion

type Database =
    inherit IDisposable
    abstract member Get : (Entity * AttributeId) -> DataSeries option
    abstract member Set : (Entity * AttributeId) -> DataSeries -> unit
    abstract member Ups : (Entity * AttributeId) -> (Date * Tx * int64) -> unit
    abstract member TryGetTextId : Text -> TextId option
    abstract member GetTextId : Text -> TextId
    abstract member GetText : TextId -> Text
    abstract member GetDataId : byte[] -> DataId
    abstract member GetData : DataId -> byte[]
    abstract member SnapshotList : unit -> Result<int[],exn>
    abstract member SnapshotSave : int -> Result<unit,exn>
    abstract member SnapshotLoad : int -> Result<unit,exn>
    abstract member SnapshotDelete : int -> Result<unit,exn>

module Database =

    let createMemory (snapshotPath:string) =
        let dataSeriesDictionary = Dictionary<Entity * AttributeId, DataSeries>()
        let dataSeriesLock = new ReaderWriterLockSlim()
        let stringDictionary = Dictionary StringComparer.Ordinal
        let texts = ResizeArray()
        let textLock = new ReaderWriterLockSlim()
        let bytes = ResizeArray()
        let byteLock = new ReaderWriterLockSlim()
        { new Database with
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
                    texts.[int i]
                finally
                    textLock.ExitReadLock()
            member __.TryGetTextId (Text s) =
                textLock.EnterWriteLock()
                try
                    let mutable i = 0u
                    if stringDictionary.TryGetValue(s, &i) then TextId i |> Some
                    else None
                finally
                    textLock.ExitWriteLock()
            member __.GetTextId (Text s) =
                textLock.EnterWriteLock()
                try
                    let mutable i = 0u
                    if stringDictionary.TryGetValue(s, &i) then TextId i
                    else
                        i <- uint32 texts.Count
                        stringDictionary.Add(s,i)
                        texts.Add (Text s)
                        TextId i
                finally
                    textLock.ExitWriteLock()
            member __.GetData (DataId i) =
                byteLock.EnterReadLock()
                try
                    bytes.[int i]
                finally
                    byteLock.ExitReadLock()
            member __.GetDataId bs =
                byteLock.EnterWriteLock()
                try
                    let i = uint32 bytes.Count
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
                        StreamSerialize.dataSeriesDictionarySet fs dataSeriesDictionary
                        StreamSerialize.textListSet fs texts
                        StreamSerialize.byteListSet fs bytes
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
                    StreamSerialize.dataSeriesDictionaryLoad fs dataSeriesDictionary
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