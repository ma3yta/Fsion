module Fsion.Tests.DatabaseTests

open System
open Expecto
open Fsion

let dataCacheTestList (cache:DataCache) = [

        testList "dataSeries" [

            testAsync "set get" {
                let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
                cache.Set (Entity(EntityType.attribute,1), Attribute.time) dataSeries
                let actual = cache.Get (Entity(EntityType.attribute,1), Attribute.time)
                Expect.equal actual (Some dataSeries) "bytes 1"
            }

            testAsync "ups" {
                let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
                cache.Set (Entity(EntityType.attribute,2), Attribute.time) dataSeries
                cache.Ups (Entity(EntityType.attribute,2), Attribute.time) (Date 2u,Tx 2u,2L)
                let actual = cache.Get (Entity(EntityType.attribute,2), Attribute.time)
                let expected = DataSeries.append (Date 2u,Tx 2u,2L) dataSeries |> Some
                Expect.equal actual expected "append"
            }
        ]

        testList "text" [

            testAsync "same id" {
                let expected = cache.GetTextId (Text.ofString "hi you")
                let actual = cache.GetTextId (Text.ofString " hi you ")
                Expect.equal actual expected "same id"
            }

            testAsync "diff id" {
                let expected = cache.GetTextId (Text.ofString "hi you")
                let actual = cache.GetTextId (Text.ofString "hi there")
                Expect.notEqual actual expected "diff id"
            }

            testAsync "case sensitive" {
                let expected = cache.GetTextId (Text.ofString "hi you")
                let actual = cache.GetTextId (Text.ofString "hi You")
                Expect.notEqual actual expected "case"
            }
        
            testProp "roundtrip" (fun (strings:string[]) ->
                let expected = Array.Parallel.map Text.ofString strings
                let textIds = Array.Parallel.map cache.GetTextId expected
                let actual = Array.Parallel.map cache.GetText textIds
                Expect.equal actual expected "strings same"
            )
        ]

        testList "data" [

            testAsync "save" {
                let expected = [|1uy;3uy|]
                let dataId = cache.GetDataId expected
                let actual = cache.GetData dataId
                Expect.equal actual expected "same id"
            }

            testAsync "diff id" {
                let expected = cache.GetDataId [|1uy;3uy|]
                let actual = cache.GetDataId [|7uy;5uy|]
                Expect.notEqual actual expected "diff id"
            }

            testProp "roundtrip" (fun (bytes:byte[][]) ->
                let byteIds = Array.Parallel.map cache.GetDataId bytes
                let actual = Array.Parallel.map cache.GetData byteIds
                Expect.equal actual bytes "bytes same"
            )
        ]
    ]

let dataCacheTests =
    DataCache.createMemory()
    |> dataCacheTestList
    |> testList "dataCache memory"

let databaseTestList (db:Database) = [
    
    testAsync "nothing" {
        let txData = {
            Headers = []
            Updates = []
            Creates = []
            Strings = [|"hi"|]
        }
        Database.setTransaction txData (Time 1L) db
    }

    testAsync "create" {
        let txData = {
            Headers = []
            Updates = []
            Creates = [EntityType.attribute, List1.ofOne (Attribute.uri, Date 10u, 0L)]
            Strings = [|"my_uri"|]
        }
        Database.setTransaction txData (Time 2L) db
    }

    testAsync "update" {
        let txData = {
            Headers = []
            Updates = [Attribute.uri.Entity, List1.ofOne (Attribute.uri, Date 10u, 0L)]
            Creates = []
            Strings = [|"my_uri2"|]
        }
        Database.setTransaction txData (Time 2L) db
    }
]

let databaseTests =
    Database.createMemory()
    |> databaseTestList
    |> testList "dataSeriesBase memory"