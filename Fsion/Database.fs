namespace Fsion

open System
open System.IO
open System.Threading
open Fsion

type Database =
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

module Database =

    let createMemory (snapshotPath:string) =
        let mutable dataSeriesMap = MapSlim()
        let mutable texts = SetSlim()
        let mutable bytes = ListSlim()
        { new Database with
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