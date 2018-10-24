module Fsion.Tests.DatabaseTests

open System
open Expecto
open Fsion

let dataSeriesTestList (cache:DataSeriesCache) = [

        testAsync "set get" {
            let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
            cache.Set (Entity(EntityType.attribute,1), Attribute.time) dataSeries
            let actual = cache.Get (Entity(EntityType.attribute,1), Attribute.time)
            Expect.equal actual dataSeries "bytes 1"
        }

        testAsync "ups" {
            let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
            cache.Set (Entity(EntityType.attribute,2), Attribute.time) dataSeries
            cache.Ups (Entity(EntityType.attribute,2), Attribute.time) (Date 2u,Tx 2u,2L)
            let actual = cache.Get (Entity(EntityType.attribute,2), Attribute.time)
            let expected = DataSeries.append (Date 2u,Tx 2u,2L) dataSeries
            Expect.equal actual expected "append"
        }
    ]

let dataSeriesTests =
    DataSeriesCache.createMemory()
    |> dataSeriesTestList
    |> testList "dataSeriesCache memory"

let textCacheTestList (cache:TextCache) = [
    
        testAsync "same id" {
            let expected = cache.GetId (Text.ofString "hi you")
            let actual = cache.GetId (Text.ofString " hi you ")
            Expect.equal actual expected "same id"
        }

        testAsync "diff id" {
            let expected = cache.GetId (Text.ofString "hi you")
            let actual = cache.GetId (Text.ofString "hi there")
            Expect.notEqual actual expected "diff id"
        }

        testAsync "case sensitive" {
            let expected = cache.GetId (Text.ofString "hi you")
            let actual = cache.GetId (Text.ofString "hi You")
            Expect.notEqual actual expected "case"
        }
        
        testProp "roundtrip" (fun (strings:string array) ->
            let expected = Array.Parallel.map Text.ofString strings
            let textIds = Array.Parallel.map cache.GetId expected
            let actual = Array.Parallel.map cache.GetText textIds
            Expect.equal actual expected "strings same"
        )
]

let textCacheTests =
    TextCache.createMemory()
    |> textCacheTestList
    |> testList "textCache memory"

let dataSeriesBaseTestList (db:DataSeriesBase) = [
    
    testAsync "nothing" {
        let txData = {
            Headers = []
            Updates = []
            Creates = []
            Strings = [|"hi"|]
        }
        DataSeriesBase.setTransaction txData (Time 1L) db
    }

    testAsync "create" {
        let txData = {
            Headers = []
            Updates = []
            Creates = [EntityType.attribute, List1.ofOne (Attribute.uri, Date 10u, 0L)]
            Strings = [|"my_uri"|]
        }
        DataSeriesBase.setTransaction txData (Time 2L) db
    }

    testAsync "update" {
        let txData = {
            Headers = []
            Updates = [Attribute.uri.Entity, List1.ofOne (Attribute.uri, Date 10u, 0L)]
            Creates = []
            Strings = [|"my_uri2"|]
        }
        DataSeriesBase.setTransaction txData (Time 2L) db
    }
]

let dataSeriesBaseTests =
    DataSeriesBase.createMemory()
    |> dataSeriesBaseTestList
    |> testList "dataSeriesBase memory"