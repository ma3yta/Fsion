namespace Fsion

module Transactor =

    [<Struct;NoComparison>]
    type internal TempSet =
        | TempSet of SetSlim<AttributeId> * (AttributeId list * AttributeId list) option

    module internal TempSet =
        let contains a (TempSet(s,o)) =
            match o with
            | None -> let ov = s.Get a in ov.IsSome
            | Some(al,rl) ->
                List.contains a rl |> not
                &&
                (let ov = s.Get a in ov.IsSome
                 || List.contains a al)

    type Store =
        abstract member Set : Transaction -> Result<unit,Text>
        abstract member Get : Tx * uint32 -> Result<Transaction array,Text>
        abstract member SetSummary : Result<Counts,Text>
        abstract member GetSummary : unit -> Result<Counts,Text>

    let private encodedTypeTextId = FsionValue.encodeType (Some TypeTextId)
    let private encodedTypeDataId = FsionValue.encodeType (Some TypeDataId)

    let inline internal noNewCounts (counts:Counts) (txn:Transaction) =
        List.isEmpty txn.Text
        && List.isEmpty txn.Data
        && List1.forall (fun (Entity((EntityType etyId) as ety,eid),_,_,_) ->
            Counts.get ety counts > eid || etyId = EntityType.Int.transaction
        ) txn.Datum

    let internal updateTextAndDataAttributes (text:SetSlim<AttributeId>) (data:SetSlim<AttributeId>) (datum:Datum list1) =
        let mutable textAdd,textRemove,dataAdd,dataRemove = [],[],[],[]
        List1.iter (function
            | (Entity(EntityType EntityType.Int.attribute,eid)),AttributeId AttributeId.Int.attribute_type,_,i ->
                let aid = AttributeId eid
                if i=encodedTypeTextId then textAdd <- aid::textAdd else textRemove <- aid::textRemove
                if i=encodedTypeDataId then dataAdd <- aid::dataAdd else dataRemove <- aid::dataRemove
            | _ -> ()
        ) datum
        let textEdit = if textAdd = [] && textRemove = [] then None else Some(textAdd,textRemove)
        let dataEdit = if dataAdd = [] && dataRemove = [] then None else Some(dataAdd,dataRemove)
        TempSet(text,textEdit), TempSet(data,dataEdit)

    let internal updateCounts (counts:Counts) (txn:Transaction) : Counts =
        let edit = Counts.toEdit counts
        Counts.update edit txn
        Counts.toCounts edit

    let internal updateTransaction (recentTxn:Transaction list1) (recentCounts:Counts list1)
                                   (textAttributes:TempSet) (dataAttributes:TempSet) (data:Transaction) =
        let nextTx = recentTxn.Head.Tx |> Tx.next
        let tx = data.Tx
        if tx = nextTx then
            if noNewCounts recentCounts.Head data then Some (data, recentCounts.Head)
            else Some (data, updateCounts recentCounts.Head data)
        elif nextTx-tx >= recentCounts.Length then None
        elif noNewCounts (List1.item (nextTx-tx) recentCounts) data then
            let data = // Just update txIds
                { data with
                    Datum =
                        let tranET = EntityType.transaction
                        let _,_,date,_ = data.Datum.Head
                        List1.toList data.Datum
                        |> List.map (fun (Entity(ety,eid),att,dat,ven) ->
                            if ety = tranET then Entity(ety,nextTx.Int),att,dat,ven
                            else Entity(ety,eid),att,dat,ven
                        )
                        |> List1.init (Entity(tranET,nextTx.Int),AttributeId.transaction_based_on,date,Some(tx.Int-1u) |> FsionValue.encodeUInt)
                }
            Some (data, recentCounts.Head)
        else
            let currentCounts = recentCounts.Head
            let originalCounts = List1.item (nextTx-tx) recentCounts
            let data = // Update txIds, seti, setText and setData
                let pastTexts =
                    if List.isEmpty data.Text then []
                    else
                        List.init (nextTx-tx) (fun i ->
                            (List1.item i recentTxn).Text
                        )
                        |> List.rev
                        |> List.concat
                let newTexts = List.except pastTexts data.Text
                { data with
                    Text = newTexts
                    Datum =
                        let tranET = EntityType.transaction
                        let _,_,date,_ = data.Datum.Head
                        List1.toList data.Datum
                        |> List.map (fun (Entity(ety,eid),att,dat,ven) ->
                            let eid =
                                if ety=tranET then nextTx.Int
                                elif Counts.get ety originalCounts > eid then eid
                                else eid + Counts.get ety currentCounts - Counts.get ety originalCounts
                            let ven =
                                if List.isEmpty data.Data |> not && TempSet.contains att dataAttributes then
                                    FsionValue.decodeUInt ven
                                    |> Option.map (fun i ->
                                        if Counts.getData originalCounts > i then i
                                        else
                                            i + Counts.getData currentCounts - Counts.getData originalCounts
                                    )
                                    |> FsionValue.encodeUInt
                                elif List.isEmpty data.Text |> not && TempSet.contains att textAttributes then
                                    FsionValue.decodeUInt ven
                                    |> Option.map (fun i ->
                                        if Counts.getText originalCounts > i then i
                                        else
                                            let text = List.item (int(i-Counts.getText originalCounts)) data.Text
                                            match List.tryFindIndex ((=)text) pastTexts with
                                            | Some j -> uint32 j + Counts.getText originalCounts
                                            | None ->
                                                uint32(List.findIndex ((=)text) newTexts)
                                                + Counts.getText currentCounts
                                    )
                                    |> FsionValue.encodeUInt
                                else ven
                            Entity(ety,eid),att,dat,ven
                        )
                        |> List1.init (Entity(tranET,nextTx.Int),AttributeId.transaction_based_on,date,Some(tx.Int-1u) |> FsionValue.encodeUInt)
                }
            Some (data, updateCounts currentCounts data)

    let commit (store:Store) recentData recentCounts textAttributes dataAttributes (txn:Transaction) =

        let texts, datas = updateTextAndDataAttributes textAttributes dataAttributes txn.Datum

        match updateTransaction recentData recentCounts texts datas txn with
        | None -> Text "Transaction age" |> Error
        | Some (data, counts) ->
            store.Set data
            |> Result.map (fun () ->
                let (TempSet(_,otext)) = texts
                let (TempSet(_,odata)) = datas
                data, counts, otext, odata
            )

// Stores
// Just imp the memory ones and local disk
// Offer a conversion from a stream store

//open System.IO
//open System.Threading.Tasks
//open System.Collections.Generic

//module TransationStore =
//    let createNull() =
//        { new TransactionStore with
//            member __.Set tx bytes =
//                Ok ()
//            member __.Get tx =
//                invalidOp "null log"
//        }
//    let createLocal transactionPath =
//        { new TransactionStore with
//            member __.Set (Tx txId) transactionData =
//                try
//                    use fs =
//                        Path.Combine [|transactionPath;txId.ToString()+".fsl"|]
//                        |> File.Create
//                    StreamSerialize.transactionDataSet fs transactionData
//                    Ok ()
//                with e -> Error e
//            member __.Get tx =
//                invalidOp "not implemented"
//        }