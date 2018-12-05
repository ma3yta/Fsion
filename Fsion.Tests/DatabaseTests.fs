module Fsion.Tests.DatabaseTests

open System
open Expecto
open Fsion

let dataCacheTestList (cache:Database) = [

        testList "dataSeries" [

            testAsync "set get" {
                let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
                cache.Set (EntityAttribute(Entity(EntityType.attribute,1u), AttributeId.time)) dataSeries
                let actual = cache.Get (EntityAttribute(Entity(EntityType.attribute,1u), AttributeId.time))
                Expect.equal actual (ValueSome dataSeries) "bytes 1"
            }

            testAsync "ups" {
                let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
                cache.Set (EntityAttribute(Entity(EntityType.attribute,2u), AttributeId.time)) dataSeries
                cache.Ups (EntityAttribute(Entity(EntityType.attribute,2u), AttributeId.time)) (Date 2u,Tx 2u,2L)
                let actual = cache.Get (EntityAttribute(Entity(EntityType.attribute,2u), AttributeId.time))
                let expected = DataSeries.append (Date 2u,Tx 2u,2L) dataSeries |> ValueSome
                Expect.equal actual expected "append"
            }
        ]

        testList "text" [

            testAsync "same id" {
                let expected = Text.ofString "hi you" |> Option.get |> cache.GetTextId
                let actual = Text.ofString " hi you "|> Option.get |> cache.GetTextId
                Expect.equal actual expected "same id"
            }

            testAsync "diff id" {
                let expected = cache.GetTextId (Text "hi you")
                let actual = cache.GetTextId (Text "hi there")
                Expect.notEqual actual expected "diff id"
            }

            testAsync "case sensitive" {
                let expected = Text "hi you" |> cache.GetTextId
                let actual = Text "hi You" |> cache.GetTextId
                Expect.notEqual actual expected "case"
            }
        
            testProp "roundtrip" (fun (texts:Text[]) ->
                let textIds = Array.Parallel.map cache.GetTextId texts
                let actual = Array.Parallel.map cache.GetText textIds
                Expect.equal actual texts "strings same"
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

        testList "snapshot" [
        
            testSequencedGroup null <| testList null [
            
                testAsync "list empty" {
                    Expect.equal (cache.SnapshotList()) (Ok [||]) "list empty"
                }
            
                testAsync "save list delete" {
                    Expect.equal (cache.SnapshotSave 23) (Ok ()) "saves"
                    Expect.equal (cache.SnapshotList()) (Ok [|23|]) "lists"
                    Expect.equal (cache.SnapshotDelete 23) (Ok ()) "deletes"
                }
            ]

            testSequenced <| testAsync "save list load delete" {
                Expect.equal (cache.SnapshotSave 29) (Ok ()) "saves"
                Expect.equal (cache.SnapshotList()) (Ok [|29|]) "lists"
                Expect.equal (cache.SnapshotLoad 29) (Ok ()) "loads"
                Expect.equal (cache.SnapshotDelete 29) (Ok ()) "deletes"
            }
        ]
    ]

let dataCacheTests =
    let tempDir = tempDir()
    afterTesting tempDir.Dispose
    let dataCache = Database.createMemory tempDir.Path
    afterTesting dataCache.Dispose
    dataCache
    |> dataCacheTestList
    |> testList "dataCache memory"

let databaseTestList (db:Transactor.Context) = [
    
    testAsync "nothing" {
        let txData = {
            Text = [|Text "hi"|]
            Data = [||]
            Creates = [||]
            EntityDatum = []
            TransactionDatum = []
        }
        Transactor.commit db txData |> ignore
    }

    testAsync "create" {
        let txData = {
            Text = [|Text "my_uri"|]
            Data = [||]
            Creates = [||]
            EntityDatum = [Entity(EntityType.attribute,1u), AttributeId.uri, Date 10u, 0L]
            TransactionDatum = []
        }
        Transactor.commit db txData |> ignore
    }

    testAsync "update" {
        let txData = {
            Text = [|Text "my_uri2"|]
            Data = [||]
            Creates = [||]
            EntityDatum = [Selector.toEntity AttributeId.uri, AttributeId.uri, Date 10u, 0L]
            TransactionDatum = []
        }
        Transactor.commit db txData |> ignore
    }
]

let databaseTests =
    let tempDir = tempDir()
    let database = Database.createMemory tempDir.Path
    let database = Transactor.localContext database
    afterTesting tempDir.Dispose
    database
    |> databaseTestList
    |> testList "dataSeriesBase memory"