module Fsion.Tests.DatabaseTests

open System
open Expecto
open FsCheck
open Fsion

let createDB() =
    DataSeriesBase.createMemory()

let databaseTests =
    testList "database" [
        testAsync "simple" {
            let db = createDB()
            db.Set (Entity(EntityType 1,1), {Id = 1; IsSet = false }) [|1uy|]
            let actual = db.Get (Entity(EntityType 1,1), {Id = 1; IsSet = false })
            Expect.equal actual [|1uy|] "bytes 1"
        }
        testAsync "ups" {
            let db = createDB()
            let (DataSeries ds) = DataSeries.single (Date 1u,Tx 1u,1L)
            db.Set (Entity(EntityType 1,1), {Id = 1; IsSet = false }) ds
            db.Ups (Entity(EntityType 1,1), {Id = 1; IsSet = false }) (Date 2u,Tx 2u,2L)
            let actual = db.Get (Entity(EntityType 1,1), {Id = 1; IsSet = false })
            let (DataSeries expected) = DataSeries.append (Date 2u,Tx 2u,2L) (DataSeries ds)
            Expect.equal actual expected "append"
        }
    ]