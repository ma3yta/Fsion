module Fsion.Tests.DatabaseTests

open System
open Expecto
open FsCheck
open Fsion

let createDB() =
    DataSeriesCache.createMemory()

let databaseTests =
    testList "database" [
        testAsync "simple" {
            let database = createDB()
            let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
            database.Set (Entity(EntityType 1,1), Attribute.time) dataSeries
            let actual = database.Get (Entity(EntityType 1,1), Attribute.time)
            Expect.equal actual dataSeries "bytes 1"
        }
        testAsync "ups" {
            let database = createDB()
            let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
            database.Set (Entity(EntityType 1,1), Attribute.time) dataSeries
            database.Ups (Entity(EntityType 1,1), Attribute.time) (Date 2u,Tx 2u,2L)
            let actual = database.Get (Entity(EntityType 1,1), Attribute.time)
            let expected = DataSeries.append (Date 2u,Tx 2u,2L) dataSeries
            Expect.equal actual expected "append"
        }
    ]