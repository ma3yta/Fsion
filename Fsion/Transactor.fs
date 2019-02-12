namespace Fsion

open System
open System.IO
open System.Threading.Tasks
open System.Collections.Generic
open Fsion

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

module Transactor =

    [<Struct>]
    type Counts =
        | Counts of uint32 array

    module internal Counts =
        [<Struct>]
        type internal Edit =
            | Edit of uint32 array ref
        let get (EntityType ety) (Counts a) =
            if Array.length a > int ety+1 then a.[int ety+1] else 0u
        let getText (Counts a) = a.[0]
        let getData (Counts a) = a.[1]
        let copy (Counts a) = Array.copy a |> ref |> Edit
        let set (EntityType ety) (Edit a) v =
            if Array.length !a <= int ety+1 then Array.Resize(a, int ety+2)
            (!a).[int ety+1] <- v
        let setText (Edit a) v = (!a).[0] <- v
        let setData (Edit a) v = (!a).[1] <- v
        let toCounts (Edit a) = Counts !a

    [<Struct;NoComparison>]
    type internal TempSet =
        | TempSet of SetSlim<AttributeId> * add: AttributeId list * remove: AttributeId list

    module internal TempSet =
        let contains a (TempSet(s,al,rl)) =
            List.contains a rl |> not
            &&
            (let ov = s.Get a in ov.IsSome
             || List.contains a al)

    type Store =
        abstract member Set : TxData -> Result<unit,Text>
        abstract member Get : Tx * uint32 -> Result<TxData array,Text>
        abstract member SetSummary : Result<Counts,Text>
        abstract member GetSummary : unit -> Result<Counts,Text>

    let private encodedTypeTextId = FsionValue.encodeType (Some FsionType.TypeTextId)
    let private encodedTypeDataId = FsionValue.encodeType (Some FsionType.TypeDataId)

    let inline internal noNewCounts (counts:Counts) (data:TxData) =
        List.isEmpty data.Text
        && List.isEmpty data.Data
        && List.forall (fun (Entity((EntityType etyId) as ety,eid),_,_,_) ->
            Counts.get ety counts > eid || etyId = EntityType.Int.transaction
        ) data.Datum

    let inline internal updateTextAndDataAttributes (text:SetSlim<AttributeId>) (data:SetSlim<AttributeId>) (datum:Datum list) =
        let mutable textAdd,textRemove,dataAdd,dataRemove = [],[],[],[]
        List.iter (function
            | (Entity(EntityType EntityType.Int.attribute,eid)),AttributeId AttributeId.Int.attribute_type,_,i ->
                let aid = AttributeId eid
                if i=encodedTypeTextId then textAdd <- aid::textAdd else textRemove <- aid::textRemove
                if i=encodedTypeDataId then dataAdd <- aid::dataAdd else dataRemove <- aid::dataRemove
            | _ -> ()
        ) datum
        TempSet(text,textAdd,textRemove), TempSet(data,dataAdd,dataRemove)

    let internal updateCounts (counts:Counts) (data:TxData) : Counts =
        let edit = Counts.copy counts
        if List.isEmpty data.Text |> not then
            Counts.getText counts
            |> (+) (uint32(List.length data.Text))
            |> Counts.setText edit
        if List.isEmpty data.Data |> not then
            Counts.getData counts
            |> (+) (uint32(List.length data.Data))
            |> Counts.setData edit
        List.iter (fun (Entity((EntityType etyId) as ety,eid),_,_,_) ->
            if Counts.get ety counts <= eid && etyId <> EntityType.Int.transaction then Counts.set ety edit (eid+1u)
        ) data.Datum
        Counts.toCounts edit

    let internal concurrencyUpdate (recentData:TxData list) (recentCounts:Counts list)
                                   (textAttributes:TempSet) (dataAttributes:TempSet) (data:TxData) =
        let Entity(_,lastTxId),_,_,_ = recentData.Head.Datum.Head
        let nextTxId = lastTxId + 1u
        let Entity(_,txId),_,_,_ = data.Datum.Head
        if txId = nextTxId then
            if noNewCounts (List.head recentCounts) data then Some (data, List.head recentCounts)
            else Some (data, updateCounts (List.head recentCounts) data)
        elif int(nextTxId-txId) >= List.length recentCounts then None
        elif noNewCounts (List.item (int(nextTxId-txId)) recentCounts) data then
            let data = // Just update txIds
                { data with
                    Datum =
                        let tranET = EntityType.transaction
                        let _,_,date,_ = data.Datum.Head
                        (Entity(tranET,nextTxId),AttributeId.transaction_based_on,date,Some(txId-1u)
                            |> FsionValue.encodeUInt)
                        :: List.map (fun (Entity(ety,eid),att,dat,ven) ->
                            if ety = tranET then Entity(ety,nextTxId),att,dat,ven
                            else Entity(ety,eid),att,dat,ven
                        ) data.Datum
                }
            Some (data, List.head recentCounts)
        else
            let currentCounts = List.head recentCounts
            let originalCounts = List.item (int(nextTxId-txId)) recentCounts
            let data = // Update txIds, seti, setText and setData
                let pastTexts =
                    if List.isEmpty data.Text then []
                    else
                        List.init (int(nextTxId-txId)) (fun i ->
                            (List.item i recentData).Text
                        )
                        |> List.rev
                        |> List.concat
                let newTexts = List.except pastTexts data.Text
                { data with
                    Text = newTexts
                    Datum =
                        let tranET = EntityType.transaction
                        let _,_,date,_ = data.Datum.Head
                        (Entity(tranET,nextTxId),AttributeId.transaction_based_on,date,Some(txId-1u)
                            |> FsionValue.encodeUInt)
                        :: List.map (fun (Entity(ety,eid),att,dat,ven) ->
                            let eid =
                                if ety=tranET then nextTxId
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
                        ) data.Datum
                }
            Some (data, updateCounts currentCounts data)

    let commit (store:Store) recentData recentCounts textAttributes dataAttributes (nextTxId:uint32) (txData:TxData) =

        let texts, datas = updateTextAndDataAttributes textAttributes dataAttributes txData.Datum

        match concurrencyUpdate recentData recentCounts texts datas txData with
        | None -> Text "Transaction age" |> Error
        | Some (data, counts) ->
            store.Set data
            |> Result.map (fun () ->
                let (TempSet(_,ta,tr)) = texts
                let (TempSet(_,da,dr)) = datas
                data, counts, (ta,tr), (da,dr)
            )

// Stores
// Just imp the memory ones and local disk
// Offer a conversion from a stream store