namespace Fsion

open System
open System.IO
open System.Threading.Tasks
open Fsion

type TransactionLog =
    abstract member Set : Tx -> TransactionData -> Result<unit,exn>
    abstract member Get : Tx -> Task<TransactionData> seq

module TransationLog =
    let createNull() =
        { new TransactionLog with
            member __.Set tx bytes =
                Ok ()
            member __.Get tx =
                invalidOp "null log"
        }
    let createLocal transactionPath =
        { new TransactionLog with
            member __.Set (Tx txId) transactionData =
                try
                    use fs =
                        Path.Combine [|transactionPath;txId.ToString()+".fsl"|]
                        |> File.Create
                    StreamSerialize.transactionDataSet fs transactionData
                    Ok ()
                with e -> Error e
            member __.Get tx =
                invalidOp "not implemented"
        }

[<NoComparison;NoEquality>]
type private TransactionLocalContext = {
    Database : Database
    mutable IndexEntityTypeCount : uint32 array
    mutable IndexEntityTypeAttribute : uint32 [][]
}

module Transactor =

    [<NoComparison;NoEquality>]
    type Context =
        private
        | Local of TransactionLocalContext

    let localContext (database:Database) =
        {
            Database = database
            IndexEntityTypeCount = [|0u;0u;0u|]
            IndexEntityTypeAttribute = [|[||];[||];[||]|]
        } |> Local

    [<Literal>]
    let private txEntityTypeIndex = 0
    [<Literal>]
    let private entityTypeEntityTypeIndex = 1

    let private transactionLock = obj()

    let private currentTime (Local db) =
        Time 1L

    let commit (Local db) (txData: TransactionData) =
        
        // TODO: change any new equities to have lower ids if this is being serialized

        lock transactionLock (fun () ->
            let time = currentTime (Local db)

            let txId = uint32 db.IndexEntityTypeCount.[txEntityTypeIndex]
            
            let ups (Entity(EntityType etId,_) as entity,AttributeId attributeId,date,value) =
                let mutable attributeArray = &db.IndexEntityTypeAttribute.[int etId]
                db.Database.Ups (EntityAttribute(entity,AttributeId attributeId)) (date,Tx txId,value)
                if Array.contains attributeId attributeArray |> not then
                    Array.Resize(&attributeArray, attributeArray.Length+1)
                    attributeArray.[attributeArray.Length-1] <- attributeId
                    //db.IndexEntityTypeAttribute.[int etId] <- attributeArray

            let date = Time.toDate time
            let txEntity = Entity(EntityType.transaction, txId)

            txData.TransactionDatum
            |> Seq.append [AttributeId.time, Time.toInt64 time]
            |> Seq.map (fun (a,v) -> txEntity,a,date,v)
            |> Seq.iter ups

            Seq.iter ups txData.EntityDatum
            

            db.IndexEntityTypeCount.[txEntityTypeIndex] <- txId + 1u
        )

        Text "transaction successful"