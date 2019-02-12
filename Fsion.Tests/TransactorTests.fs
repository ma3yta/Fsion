module Fsion.Tests.TransactorTests

open System
open Expecto
open Fsion
open Transactor

let transactorTests =

    let tempSet l = TempSet(SetSlim(),l,[])
    let emptyTx i v = {
        Text = []
        Data = []
        Datum = [Entity(EntityType.transaction,i),AttributeId.time,Date.minValue,v]
    }

    testList "transactor" [
        
        testAsync "no data current tx" {
            let recentData = [emptyTx 123u 0L]
            let txData = emptyTx 124u 0L
            let recentCounts = [Counts [||]]
            let texts = tempSet []
            let datas = tempSet []
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            Expect.equal data (Some(txData,Counts[||])) "same"
        }

        testAsync "no data last tx" {
            let recentData = [emptyTx 123u 0L]
            let txData = emptyTx 123u 0L
            let recentCounts = [Counts [||];Counts [||]]
            let texts = tempSet []
            let datas = tempSet []
            let data = concurrencyUpdate recentData recentCounts texts datas txData
            let expectedData = {
                txData with
                    Datum = [
                        Entity(EntityType.transaction,124u),AttributeId.transaction_based_on,Date.minValue,FsionValue.encodeUInt(Some 122u)
                        Entity(EntityType.transaction,124u),AttributeId.time,Date.minValue,0L
                    ]
            }
            Expect.equal data (Some(expectedData,Counts[||])) "same"
        }
    ]


// ALWAYS
// TxData need to be checked for new Text and Data fields and any changed from - DONE

// CURRENT TRANSACTION
// TxData need to be checked for count increases - DONE

// OLD TRANSACTION
// TxData transaction datum need to be adjusted - DONE
// TxData needs transaction_based_on added - DONE
// TxData need to be checked for new entity adjustments and count increases - DONE
// TxData need to be checked for new Text adjustments - DONE
// TxData need to be checked for new Data adjustments - DONE
