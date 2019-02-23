module Fsion.Tests.SelectorTests

open System
open Expecto
open Fsion

let dataStoreTestList (store:Selector.Store) = [

        testList "dataSeries" [

            //testAsync "set get" {
            //    let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
            //    store.Set (EntityAttribute(Entity(EntityType.attribute,1u), AttributeId.time)) dataSeries
            //    let actual = store.Get (EntityAttribute(Entity(EntityType.attribute,1u), AttributeId.time))
            //    Expect.equal actual (ValueSome dataSeries) "bytes 1"
            //}

            //testAsync "ups" {
            //    let dataSeries = DataSeries.single (Date 1u,Tx 1u,1L)
            //    store.Set (EntityAttribute(Entity(EntityType.attribute,2u), AttributeId.time)) dataSeries
            //    store.Ups (EntityAttribute(Entity(EntityType.attribute,2u), AttributeId.time)) (Date 2u,Tx 2u,2L)
            //    let actual = store.Get (EntityAttribute(Entity(EntityType.attribute,2u), AttributeId.time))
            //    let expected = DataSeries.append (Date 2u,Tx 2u,2L) dataSeries |> ValueSome
            //    Expect.equal actual expected "append"
            //}
        ]

        testList "text" [

            testAsync "same id" {
                let expected = Text.ofString "hi you" |> Option.get |> store.GetTextId
                let actual = Text.ofString " hi you "|> Option.get |> store.GetTextId
                Expect.equal actual expected "same id"
            }

            testAsync "diff id" {
                let expected = store.GetTextId (Text "hi you")
                let actual = store.GetTextId (Text "hi there")
                Expect.notEqual actual expected "diff id"
            }

            testAsync "case sensitive" {
                let expected = Text "hi you" |> store.GetTextId
                let actual = Text "hi You" |> store.GetTextId
                Expect.notEqual actual expected "case"
            }
        
            testProp "roundtrip" (fun (texts:Text[]) ->
                let textIds = Array.Parallel.map store.GetTextId texts
                let actual = Array.Parallel.map store.GetText textIds
                Expect.equal actual texts "strings same"
            )
        ]

        testList "data" [

            testAsync "save" {
                let expected = Data [|1uy;3uy|]
                let dataId = store.GetDataId expected
                let actual = store.GetData dataId
                Expect.equal actual expected "same id"
            }

            testAsync "diff id" {
                let expected = Data [|1uy;3uy|] |> store.GetDataId
                let actual = Data [|7uy;5uy|]|> store.GetDataId
                Expect.notEqual actual expected "diff id"
            }

            testProp "roundtrip" (fun (data:Data[]) ->
                let byteIds = Array.Parallel.map store.GetDataId data
                let actual = Array.Parallel.map store.GetData byteIds
                Expect.equal actual data "bytes same"
            )
        ]

        testList "snapshot" [
        
            //testSequencedGroup null <| testList null [
            
            //    testAsync "list empty" {
            //        Expect.equal (store.SnapshotList()) (Ok [||]) "list empty"
            //    }
            
            //    testAsync "save list delete" {
            //        Expect.equal (store.SnapshotSave 23) (Ok ()) "saves"
            //        Expect.equal (store.SnapshotList()) (Ok [|23|]) "lists"
            //        Expect.equal (store.SnapshotDelete 23) (Ok ()) "deletes"
            //    }
            //]

            //testSequenced <| testAsync "save list load delete" {
            //    Expect.equal (store.SnapshotSave 29) (Ok ()) "saves"
            //    Expect.equal (store.SnapshotList()) (Ok [|29|]) "lists"
            //    Expect.equal (store.SnapshotLoad 29) (Ok ()) "loads"
            //    Expect.equal (store.SnapshotDelete 29) (Ok ()) "deletes"
            //}
        ]
    ]

let dataCacheTests =
    let tempDir = tempDir()
    afterTesting tempDir.Dispose
    let dataCache = Selector.createMemory tempDir.Path
    afterTesting dataCache.Dispose
    dataCache
    |> dataStoreTestList
    |> testList "dataCache memory"

//let databaseTestList (db:Transactor.Context) = [
    
//    testAsync "nothing" {
//        let txData = {
//            Text = [|Text "hi"|]
//            Data = [||]
//            Creates = [||]
//            EntityDatum = []
//            TransactionDatum = []
//        }
//        Transactor.commit db txData |> ignore
//    }

//    testAsync "create" {
//        let txData = {
//            Text = [|Text "my_uri"|]
//            Data = [||]
//            Creates = [||]
//            EntityDatum = [Entity(EntityType.attribute,1u), AttributeId.uri, Date 10u, 0L]
//            TransactionDatum = []
//        }
//        Transactor.commit db txData |> ignore
//    }

//    testAsync "update" {
//        let txData = {
//            Text = [|Text "my_uri2"|]
//            Data = [||]
//            Creates = [||]
//            EntityDatum = [Selector.toEntity AttributeId.uri, AttributeId.uri, Date 10u, 0L]
//            TransactionDatum = []
//        }
//        Transactor.commit db txData |> ignore
//    }
//]

//let databaseTests =
//    let tempDir = tempDir()
//    let database = Database.createMemory tempDir.Path
//    let database = Transactor.localContext database
//    afterTesting tempDir.Dispose
//    database
//    |> databaseTestList
//    |> testList "dataSeriesBase memory"