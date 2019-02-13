module Fsion.Tests.TransactorTests

open Expecto
open Fsion
open Transactor

let transactorTests =

    let emptyTempSet = TempSet(SetSlim(),None)
    let emptyTx i v = {
        Text = []
        Data = []
        Datum = List1.singleton (Entity(EntityType.transaction,i),AttributeId.time,Date.minValue,v)
    }

    testList "transactor" [

        testAsync "new text attribute" {
            let datum = Entity(EntityType.attribute,11u),AttributeId.attribute_type,Date.minValue,FsionValue.encodeType(Some TypeTextId)
            let emptySet = SetSlim()
            let x = updateTextAndDataAttributes emptySet emptySet (List1.singleton datum)
            Expect.equal x (TempSet(emptySet,Some([AttributeId 11u],[])),TempSet(emptySet,Some([],[AttributeId 11u]))) ""
        }

        testAsync "new data attribute" {
            let datum = Entity(EntityType.attribute,11u),AttributeId.attribute_type,Date.minValue,FsionValue.encodeType(Some TypeDataId)
            let emptySet = SetSlim()
            let x = updateTextAndDataAttributes emptySet emptySet (List1.singleton datum)
            Expect.equal x (TempSet(emptySet,Some([],[AttributeId 11u])),TempSet(emptySet,Some([AttributeId 11u],[]))) ""
        }
        
        testAsync "current tx" {
            let recentData = emptyTx 123u 0L |> List1.singleton
            let recentCounts = List1.singleton Counts.empty
            let texts,datas = emptyTempSet, emptyTempSet
            let txData = emptyTx 124u 0L
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            Expect.equal data (Some(txData,Counts.empty)) ""
        }

        testAsync "last tx" {
            let recentData = emptyTx 123u 0L |> List1.singleton
            let recentCounts = List1.init Counts.empty [Counts.empty]
            let texts,datas = emptyTempSet, emptyTempSet
            let txData = emptyTx 123u 0L
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            let expectedData = {
                txData with
                    Datum = [
                        Entity(EntityType.transaction,124u),AttributeId.transaction_based_on,Date.minValue,FsionValue.encodeUInt(Some 122u)
                        Entity(EntityType.transaction,124u),AttributeId.time,Date.minValue,0L
                    ] |> List1.tryOfList |> Option.get
            }
            Expect.equal data (Some(expectedData,Counts.empty)) ""
        }

        testAsync "too old" {
            let recentData = emptyTx 123u 0L |> List1.singleton
            let recentCounts = List1.singleton Counts.empty
            let texts,datas = emptyTempSet, emptyTempSet
            let txData = emptyTx 122u 0L
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            Expect.equal data None ""
        }

        testAsync "text" {
            let recentData = emptyTx 123u 0L |> List1.singleton
            let recentCounts = List1.singleton Counts.empty
            let texts,datas = emptyTempSet, emptyTempSet
            let txData = emptyTx 124u 0L
            let txData = {txData with Text = [Text "hi"]}
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            Expect.equal data (Some(txData,Counts [|1u;0u|])) ""
        }

        testAsync "data" {
            let recentData = emptyTx 123u 0L |> List1.singleton
            let recentCounts = List1.singleton Counts.empty
            let texts,datas = emptyTempSet, emptyTempSet
            let txData = emptyTx 124u 0L
            let txData = {txData with Data = [Data [|128uy|]]}
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            Expect.equal data (Some(txData,Counts [|0u;1u|])) ""
        }

        testAsync "new entity" {
            let recentData = emptyTx 123u 0L |> List1.singleton
            let recentCounts = List1.singleton Counts.empty
            let texts,datas = emptyTempSet, emptyTempSet
            let txData = emptyTx 124u 0L
            let txData = {
                txData with
                    Datum = [
                        List1.head txData.Datum
                        Entity(EntityType.attribute,0u),AttributeId.attribute_type,Date.minValue,0L
                    ] |> List1.tryOfList |> Option.get
            }
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            Expect.equal data (Some(txData,Counts [|0u;0u;1u|])) ""
        }

        testAsync "move text" {
            let recentData = List1.init (emptyTx 123u 0L) [emptyTx 122u 0L]
            let recentCounts = List1.init (Counts [|7u;0u;1u|]) [Counts [|6u;0u;1u|]]
            let texts,datas = TempSet(SetSlim(),Some([AttributeId.name],[])), emptyTempSet
            let txData = {
                Text = [Text "hi"]
                Data = []
                Datum = [
                    Entity(EntityType.transaction,123u),AttributeId.time,Date.minValue,0L
                    Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 6u)
                ] |> List1.tryOfList |> Option.get
            }
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            let expectedData = {
                txData with
                    Datum = [
                        Entity(EntityType.transaction,124u),AttributeId.transaction_based_on,Date.minValue,FsionValue.encodeUInt(Some 122u)
                        Entity(EntityType.transaction,124u),AttributeId.time,Date.minValue,0L
                        Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 7u)
                    ] |> List1.tryOfList |> Option.get
            }
            Expect.equal data (Some(expectedData,Counts [|8u;0u;1u|])) ""
        }

        testAsync "move data" {
            let recentData = List1.init (emptyTx 123u 0L) [emptyTx 122u 0L]
            let recentCounts = List1.init (Counts [|0u;7u;0u|]) [Counts [|0u;6u;0u|]]
            let texts,datas = emptyTempSet, TempSet(SetSlim(),Some([AttributeId.name],[]))
            let txData = {
                Text = []
                Data = [Data [|167uy|]]
                Datum = [
                    Entity(EntityType.transaction,123u),AttributeId.time,Date.minValue,0L
                    Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 6u)
                ] |> List1.tryOfList |> Option.get
            }
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            let expectedData = {
                txData with
                    Datum = [
                        Entity(EntityType.transaction,124u),AttributeId.transaction_based_on,Date.minValue,FsionValue.encodeUInt(Some 122u)
                        Entity(EntityType.transaction,124u),AttributeId.time,Date.minValue,0L
                        Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 7u)
                    ] |> List1.tryOfList |> Option.get
            }
            Expect.equal data (Some(expectedData,Counts [|0u;8u;1u|])) ""
        }

        testAsync "move entity" {
            let recentData = List1.init (emptyTx 123u 0L) [emptyTx 122u 0L]
            let recentCounts = List1.init (Counts [|0u;0u;1u|]) [Counts [|0u;0u;0u|]]
            let texts,datas = emptyTempSet, emptyTempSet
            let txData = {
                Text = []
                Data = []
                Datum = [
                    Entity(EntityType.transaction,123u),AttributeId.time,Date.minValue,0L
                    Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,0L
                ] |> List1.tryOfList |> Option.get
            }
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            let expectedData = {
                txData with
                    Datum = [
                        Entity(EntityType.transaction,124u),AttributeId.transaction_based_on,Date.minValue,FsionValue.encodeUInt(Some 122u)
                        Entity(EntityType.transaction,124u),AttributeId.time,Date.minValue,0L
                        Entity(EntityType.attribute,1u),AttributeId.name,Date.minValue,0L
                    ] |> List1.tryOfList |> Option.get
            }
            Expect.equal data (Some(expectedData,Counts [|0u;0u;2u|])) ""
        }

        testAsync "move text dup" {
            let missedData = {emptyTx 123u 0L with Text = [Text "hi"]}
            let recentData = List1.init missedData [emptyTx 122u 0L]
            let recentCounts = List1.init (Counts [|7u;0u;1u|]) [Counts [|6u;0u;1u|]]
            let texts,datas = TempSet(SetSlim(),Some([AttributeId.name],[])), emptyTempSet
            let txData = {
                Text = [Text "hi"]
                Data = []
                Datum = [
                    Entity(EntityType.transaction,123u),AttributeId.time,Date.minValue,0L
                    Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 6u)
                ] |> List1.tryOfList |> Option.get
            }
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            let expectedData = {
                Text = []
                Data = []
                Datum = [
                    Entity(EntityType.transaction,124u),AttributeId.transaction_based_on,Date.minValue,FsionValue.encodeUInt(Some 122u)
                    Entity(EntityType.transaction,124u),AttributeId.time,Date.minValue,0L
                    Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 6u)
                ] |> List1.tryOfList |> Option.get
            }
            Expect.equal data (Some(expectedData,Counts [|7u;0u;1u|])) ""
        }

        testAsync "all" {
            let missedData = {emptyTx 123u 0L with Text = [Text "one";Text "two"]}
            let recentData = List1.init missedData [emptyTx 122u 0L]
            let recentCounts = List1.init (Counts [|8u;6u;2u|]) [Counts [|6u;5u;1u|]]
            let texts,datas = TempSet(SetSlim(),Some([AttributeId.name],[])), TempSet(SetSlim(),Some([AttributeId.attribute_isset],[]))
            let txData = {
                Text = [Text "three";Text "one";Text "four"]
                Data = [Data [|45uy|]]
                Datum = [
                    Entity(EntityType.transaction,123u),AttributeId.time,Date.minValue,0L
                    Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 6u)
                    Entity(EntityType.attribute,1u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 7u)
                    Entity(EntityType.attribute,1u),AttributeId.attribute_isset,Date.minValue,FsionValue.encodeUInt(Some 5u)
                ] |> List1.tryOfList |> Option.get
            }
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            let expectedData = {
                Text = [Text "three";Text "four"]
                Data = [Data [|45uy|]]
                Datum = [
                    Entity(EntityType.transaction,124u),AttributeId.transaction_based_on,Date.minValue,FsionValue.encodeUInt(Some 122u)
                    Entity(EntityType.transaction,124u),AttributeId.time,Date.minValue,0L
                    Entity(EntityType.attribute,0u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 8u)
                    Entity(EntityType.attribute,2u),AttributeId.name,Date.minValue,FsionValue.encodeUInt(Some 6u)
                    Entity(EntityType.attribute,2u),AttributeId.attribute_isset,Date.minValue,FsionValue.encodeUInt(Some 6u)
                ] |> List1.tryOfList |> Option.get
            }
            Expect.equal data (Some(expectedData,Counts [|10u;7u;3u|])) ""
        }
    ]