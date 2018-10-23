﻿namespace Fsion

open System
open System.Threading
open System.Collections.Generic
open System.Diagnostics

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
} with
    static member uri = {
        Id = 0
        Uri = "uri"
        IsSet = false
        IsString = true
    }
    static member time = {
        Id = 1
        Uri = "time"
        IsSet = false
        IsString = false
    }
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
        
type DataSeriesCache =
    abstract member Get : (Entity * Attribute) -> DataSeries // now need to be option
    abstract member Set : (Entity * Attribute) -> DataSeries -> unit
    abstract member Ups : (Entity * Attribute) -> Datum -> unit

module DataSeriesCache =
    let createMemory() =
        let dictionary = Dictionary<Entity * Attribute, DataSeries>()
        let lock = new ReaderWriterLockSlim()
        { new DataSeriesCache with
            member __.Get entityAttribute =
                try
                    lock.EnterReadLock()
                    dictionary.[entityAttribute]
                finally
                    lock.ExitReadLock()
            member __.Set entityAttribute dataSeries =
                try
                    lock.EnterWriteLock()
                    dictionary.[entityAttribute] <- dataSeries
                finally
                    lock.ExitWriteLock()
            member __.Ups entityAttribute datum =
                try
                    lock.EnterWriteLock()
                    dictionary.[entityAttribute] <-
                        match dictionary.TryGetValue entityAttribute with
                        | true, dataSeries ->
                            DataSeries.append datum dataSeries
                        | false, _ ->
                            DataSeries.single datum
                finally
                    lock.ExitWriteLock()
        }

type StringId = StringId of int

type StringCache =
    abstract member Get : StringId -> string
    abstract member Get : string -> StringId

module StringCache =
    let createMemory() =
        let dictionary = Dictionary<string, int>()
        let strings = List<string>();
        let lock = new ReaderWriterLockSlim()
        { new StringCache with
            member __.Get (StringId i) =
                lock.EnterReadLock()
                try
                    strings.[i]
                finally
                    lock.ExitWriteLock()
            member __.Get (s:string) =
                lock.EnterWriteLock()
                try
                    let mutable i = 0
                    if dictionary.TryGetValue(s, &i) then StringId i
                    else
                        i <- strings.Count
                        strings.Add(s)
                        StringId i
                finally
                    lock.ExitWriteLock()
        }

type TxData = {
    Headers: (Attribute * int64) list
    Updates: (Entity * (Attribute * Date * int64) list1) list
    Creates: (EntityType * (Attribute * Date * int64) list1) list
    Strings: string array
}

type TransactionLog =
    abstract member Set : Tx -> TxData -> unit
    abstract member Get : Tx -> TxData

module TransationLog =
    let createNull() =
        { new TransactionLog with
            member __.Set tx bytes = ()
            member __.Get tx = invalidOp "null log"
        }
    let createMemory() =
        let dictionary = Dictionary<Tx,TxData>()
        { new TransactionLog with
            member __.Set tx bytes =
                dictionary.[tx] <- bytes
            member __.Get tx =
                dictionary.[tx]
        }

type DataSeriesBase = private {
    DataSeries : DataSeriesCache
    StringCache : StringCache
    mutable IndexEntityTypeCount : int array
    mutable IndexEntityTypeAttribute : int [][]
}

module DataSeriesBase =
    let createMemory() =
        {
            DataSeries = DataSeriesCache.createMemory()
            StringCache = StringCache.createMemory()
            IndexEntityTypeCount = [|0;0;0|]
            IndexEntityTypeAttribute = [||]
        }

    [<Literal>]
    let private txEntityTypeIndex = 0
    [<Literal>]
    let private entityTypeEntityTypeIndex = 1

    let transactionLock = obj()

    let setTransaction (txData: TxData) (time: Time) (db:DataSeriesBase) =
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
                            let (StringId sid) =
                                db.StringCache.Get txData.Strings.[int value]
                            int64 sid
                        else value
                    db.DataSeries.Ups (entity,attribute)
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