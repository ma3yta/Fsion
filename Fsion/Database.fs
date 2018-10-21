namespace Fsion

open System
open System.Threading
open System.Collections.Generic

[<Struct>]
type EntityType =
    | EntityType of int

[<Struct>]
type Entity =
    | Entity of EntityType * int

[<CustomEquality;CustomComparison>]
type Attribute = {
    Id: int
    IsSet: bool
} with
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

module Attribute =
    let ups (attribute:Attribute) datum bytes =
        let (DataSeries b) = DataSeries.append datum (DataSeries bytes)
        b
        
type DataSeriesBase =
    abstract member Get : (Entity * Attribute) -> byte[]
    abstract member Set : (Entity * Attribute) -> byte[] -> unit
    abstract member Ups : (Entity * Attribute) -> Datum -> unit

module DataSeriesBase =
    let createMemory() =
        let dictionary = Dictionary<Entity * Attribute, byte[]>()
        let lock = new ReaderWriterLockSlim()
        { new DataSeriesBase with
            member __.Get entityAttribute =
                try
                    lock.EnterReadLock()
                    dictionary.[entityAttribute]
                finally
                    lock.ExitReadLock()
            member __.Set entityAttribute bytes =
                try
                    lock.EnterWriteLock()
                    dictionary.[entityAttribute] <- bytes
                finally
                    lock.ExitWriteLock()
            member __.Ups entityAttribute datum =
                try
                    lock.EnterWriteLock()
                    let newBytes =
                        dictionary.[entityAttribute]
                        |> Attribute.ups (snd entityAttribute) datum
                    dictionary.[entityAttribute] <- newBytes
                finally
                    lock.ExitWriteLock()
        }

type TransactionLog =
    abstract member Set : Tx -> byte[] -> unit
    abstract member Get : Tx -> byte[]

module TransationLog =
    let createNull() =
        { new TransactionLog with
            member __.Set tx bytes = ()
            member __.Get tx = invalidOp "null log"
        }
    let createMemory() =
        let dictionary = Dictionary<Tx,byte[]>()
        { new TransactionLog with
            member __.Set tx bytes =
                dictionary.[tx] <- bytes
            member __.Get tx =
                dictionary.[tx]
        }