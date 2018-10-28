module Fsion.Tests.ArraySerializeTests

open System
open Expecto
open FsCheck
open Fsion

let serializeTests =

    let testRoundtrip serializeSet serializeGet i =
            let b,_ = serializeSet i ArraySerialize.empty
            let j,_ = serializeGet (b,0)
            Expect.equal j i "testRoundtrip same"

    testList "serialize" [

        testAsync "zigzag examples" {
                ArraySerialize.zigzag  0 |> Expect.equal 0u <| "0"
                ArraySerialize.zigzag -1 |> Expect.equal 1u <| "-1"
                ArraySerialize.zigzag  1 |> Expect.equal 2u <| "1"
                ArraySerialize.zigzag -2 |> Expect.equal 3u <| "-2"
                ArraySerialize.zigzag  2 |> Expect.equal 4u <| "2"
                ArraySerialize.zigzag  Int32.MaxValue
                |> Expect.equal (UInt32.MaxValue-1u) <| "max-1"
                ArraySerialize.zigzag  Int32.MinValue
                |> Expect.equal UInt32.MaxValue <| "max"
        }

        testAsync "unzigzag examples" {
                ArraySerialize.unzigzag 0u |> Expect.equal  0 <| "0"
                ArraySerialize.unzigzag 1u |> Expect.equal -1 <| "1"
                ArraySerialize.unzigzag 2u |> Expect.equal  1 <| "2"
                ArraySerialize.unzigzag 3u |> Expect.equal -2 <| "3"
                ArraySerialize.unzigzag 4u |> Expect.equal  2 <| "4"
                ArraySerialize.unzigzag (UInt32.MaxValue-1u)
                |> Expect.equal Int32.MaxValue <| "max-1"
                ArraySerialize.unzigzag UInt32.MaxValue
                |> Expect.equal Int32.MinValue <| "max"
        }

        testProp "zigzag roundtrip" (fun (DoNotSize i) ->
            let j = ArraySerialize.zigzag i |> ArraySerialize.unzigzag
            Expect.equal j i "same"
        )

        testAsync "zigzag64 examples" {
                ArraySerialize.zigzag64  0L |> Expect.equal 0UL <| "0"
                ArraySerialize.zigzag64 -1L |> Expect.equal 1UL <| "-1"
                ArraySerialize.zigzag64  1L |> Expect.equal 2UL <| "1"
                ArraySerialize.zigzag64 -2L |> Expect.equal 3UL <| "-2"
                ArraySerialize.zigzag64  2L |> Expect.equal 4UL <| "2"
                ArraySerialize.zigzag64  Int64.MaxValue
                |> Expect.equal (UInt64.MaxValue-1UL) <| "max-1"
                ArraySerialize.zigzag64 Int64.MinValue
                |> Expect.equal UInt64.MaxValue <| "max"
        }

        testAsync "unzigzag64 examples" {
                ArraySerialize.unzigzag64 0UL |> Expect.equal  0L <| "0"
                ArraySerialize.unzigzag64 1UL |> Expect.equal -1L <| "1"
                ArraySerialize.unzigzag64 2UL |> Expect.equal  1L <| "2"
                ArraySerialize.unzigzag64 3UL |> Expect.equal -2L <| "3"
                ArraySerialize.unzigzag64 4UL |> Expect.equal  2L <| "4"
                ArraySerialize.unzigzag64 (UInt64.MaxValue-1UL)
                |> Expect.equal Int64.MaxValue <| "max-1"
                ArraySerialize.unzigzag64 UInt64.MaxValue
                |> Expect.equal Int64.MinValue <| "max"
        }

        testProp "zigzag64 roundtrip" (fun (DoNotSize i) ->
            let j = ArraySerialize.zigzag64 i |> ArraySerialize.unzigzag64
            Expect.equal j i "same"
        )

        testProp "bool roundtrip" (
            testRoundtrip ArraySerialize.boolSet ArraySerialize.boolGet
        )

        testProp "varint16 roundtrip" (fun (DoNotSize i) ->
            testRoundtrip ArraySerialize.uint16Set ArraySerialize.uint16Get i
        )

        testProp "varint32 roundtrip" (fun (DoNotSize i) ->
            testRoundtrip ArraySerialize.uint32Set ArraySerialize.uint32Get i
        )

        testProp "varint32 skip" (fun (DoNotSize i) ->
            let bs,i = ArraySerialize.uint32Set i ArraySerialize.empty
            let j = ArraySerialize.uint32Skip (bs,0)
            Expect.equal i j "skip same"
        )

        testProp "varint64 roundtrip" (fun (DoNotSize i) ->
            testRoundtrip ArraySerialize.uint64Set ArraySerialize.uint64Get i
        )
    ]

let dataSeriesTests =

    testList "dataSeries" [
        
        testAsync "one get" {
            let dataSeries = DataSeries.single (Date 11u,Tx 17u,13L)
            let exact = DataSeries.get (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact (Date 11u, Tx 17u, 13L) "exact"
            let before = DataSeries.get (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (Date 11u, Tx 17u, 13L) "before"
            let after = DataSeries.get (Date 12u) Tx.maxValue dataSeries
            Expect.equal after (Date 11u, Tx 17u, 13L) "after"
        }

        testAsync "two get" {
            let dataSeries =
                DataSeries.single (Date 11u,Tx 17u,13L)
                |> DataSeries.append (Date 19u,Tx 29u,23L)
            let exact1 = DataSeries.get (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact1 (Date 11u, Tx 17u, 13L) "exact1"
            let exact2 = DataSeries.get (Date 19u) Tx.maxValue dataSeries
            Expect.equal exact2 (Date 19u, Tx 29u, 23L) "exact2"
            let before = DataSeries.get (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (Date 11u, Tx 17u, 13L) "before"
            let between = DataSeries.get (Date 15u) Tx.maxValue dataSeries
            Expect.equal between (Date 11u, Tx 17u, 13L) "between"
            let after = DataSeries.get (Date 20u) Tx.maxValue dataSeries
            Expect.equal after (Date 19u, Tx 29u, 23L) "after"
        }

        testAsync "three get" {
            let dataSeries =
                DataSeries.single (Date 11u,Tx 17u,13L)
                |> DataSeries.append (Date 19u,Tx 29u,23L)
                |> DataSeries.append (Date 17u,Tx 37u,31L)
            let exact1 = DataSeries.get (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact1 (Date 11u, Tx 17u, 13L) "exact1"
            let exact2 = DataSeries.get (Date 19u) Tx.maxValue dataSeries
            Expect.equal exact2 (Date 19u, Tx 29u, 23L) "exact2"
            let exact3 = DataSeries.get (Date 17u) Tx.maxValue dataSeries
            Expect.equal exact3 (Date 17u, Tx 37u, 31L) "exact3"
            let before = DataSeries.get (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (Date 11u, Tx 17u, 13L) "before"
            let middle1 = DataSeries.get (Date 15u) Tx.maxValue dataSeries
            Expect.equal middle1 (Date 11u, Tx 17u, 13L) "middle1"
            let middle2 = DataSeries.get (Date 18u) Tx.maxValue dataSeries
            Expect.equal middle2 (Date 17u, Tx 37u, 31L) "middle2"
            let after = DataSeries.get (Date 20u) Tx.maxValue dataSeries
            Expect.equal after (Date 19u, Tx 29u, 23L) "after"
        }

        testProp "vs map" (fun (first: Date * Tx * int64,
                                list: (Date * Tx * int64) list) ->
            let dataSeries =
                DataSeries.single first
                |> List.foldBack DataSeries.append list

            let map =
                List.sort (first :: list)
                |> List.fold (fun m (dt,tx,vl) -> Map.add dt (tx,vl) m) Map.empty
            
            let minDate, (minTx, minValue) = Map.toSeq map |> Seq.head
            if minDate > Date.minValue then
                let before = DataSeries.get (minDate-1) Tx.maxValue dataSeries
                Expect.equal before (minDate, minTx, minValue) "before"

            Map.iter (fun eDate (eTx,eValue) ->
                let actual = DataSeries.get eDate Tx.maxValue dataSeries
                Expect.equal actual (eDate, eTx, eValue) "actual"

                let nextDate = eDate+1
                let next = DataSeries.get nextDate Tx.maxValue dataSeries
                let nextDateMap, (nextValueMap, nextTxMap) =
                    Map.tryFind nextDate map
                    |> Option.map (fun s -> nextDate, s)
                    |> Option.defaultValue (eDate, (eTx,eValue))
                Expect.equal next (nextDateMap, nextValueMap, nextTxMap) "next"
            ) map
        )

        testProp "tx query same as filtering first"
            (fun (first: Date * Tx * int64,
                  list: (Date * Tx * int64) list,
                  queryDate: Date,
                  queryTx: Tx) ->
            
            let dataSeriesFull =
                DataSeries.single first
                |> List.foldBack DataSeries.append list

            let dataSeriesFiltered =
                let filteredList =
                    first :: list
                    |> List.where (fun (_,tx,_) -> tx <= queryTx)
                match filteredList with
                | [] ->
                    List.minBy (fun (d,t,v) -> t,d,v) (first :: list)
                    |> DataSeries.single
                | h::l ->
                    DataSeries.single h
                    |> List.foldBack DataSeries.append l

            let full = DataSeries.get queryDate queryTx dataSeriesFull
            let filtered = DataSeries.get queryDate queryTx dataSeriesFiltered

            Expect.equal full filtered "same"
        )

        testAsync "one set get" {
            let dataSeries =
                DataSeries.setAdd (Date 11u,Tx 17u,13UL)
                |> DataSeries.single
            let exact = DataSeries.setGet (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact (set [13UL]) "exact"
            let before = DataSeries.setGet (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (set []) "before"
            let after = DataSeries.setGet (Date 12u) Tx.maxValue dataSeries
            Expect.equal after (set [13UL]) "after"
        }

        testAsync "two set get" {
            let dataSeries =
                DataSeries.setAdd (Date 11u,Tx 17u,13UL)
                |> DataSeries.single
                |> DataSeries.append (DataSeries.setAdd (Date 19u,Tx 29u,23UL))
            let exact1 = DataSeries.setGet (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact1 (set [13UL]) "exact1"
            let exact2 = DataSeries.setGet (Date 19u) Tx.maxValue dataSeries
            Expect.equal exact2 (set [13UL;23UL]) "exact2"
            let before = DataSeries.setGet (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (set []) "before"
            let between = DataSeries.setGet (Date 15u) Tx.maxValue dataSeries
            Expect.equal between (set [13UL]) "between"
            let after = DataSeries.setGet (Date 20u) Tx.maxValue dataSeries
            Expect.equal after (set [13UL;23UL]) "after"
        }

        testAsync "three set get" {
            let dataSeries =
                DataSeries.setAdd (Date 11u,Tx 17u,13UL)
                |> DataSeries.single
                |> DataSeries.append (DataSeries.setAdd (Date 19u,Tx 29u,23UL))
                |> DataSeries.append (DataSeries.setAdd (Date 17u,Tx 37u,31UL))
            let exact1 = DataSeries.setGet (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact1 (set [13UL]) "exact1"
            let exact2 = DataSeries.setGet (Date 19u) Tx.maxValue dataSeries
            Expect.equal exact2 (set [13UL;23UL;31UL]) "exact2"
            let exact3 = DataSeries.setGet (Date 17u) Tx.maxValue dataSeries
            Expect.equal exact3 (set [13UL;31UL]) "exact3"
            let before = DataSeries.setGet (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (set []) "before"
            let middle1 = DataSeries.setGet (Date 15u) Tx.maxValue dataSeries
            Expect.equal middle1 (set [13UL]) "middle1"
            let middle2 = DataSeries.setGet (Date 18u) Tx.maxValue dataSeries
            Expect.equal middle2 (set [13UL;31UL]) "middle2"
            let after = DataSeries.setGet (Date 20u) Tx.maxValue dataSeries
            Expect.equal after (set [13UL;23UL;31UL]) "after"
        }

        testAsync "set get remove" {
            let dataSeries =
                DataSeries.setAdd (Date 11u,Tx 17u,13UL)
                |> DataSeries.single
                |> DataSeries.append (DataSeries.setRemove (Date 17u,Tx 29u,13UL))
                |> DataSeries.append (DataSeries.setAdd (Date 19u,Tx 37u,13UL))
            let exact1 = DataSeries.setGet (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact1 (set [13UL]) "exact1"
            let exact2 = DataSeries.setGet (Date 17u) Tx.maxValue dataSeries
            Expect.equal exact2 (set []) "exact2"
            let exact3 = DataSeries.setGet (Date 19u) Tx.maxValue dataSeries
            Expect.equal exact3 (set [13UL]) "exact3"
            let before = DataSeries.setGet (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (set []) "before"
            let middle1 = DataSeries.setGet (Date 15u) Tx.maxValue dataSeries
            Expect.equal middle1 (set [13UL]) "middle1"
            let middle2 = DataSeries.setGet (Date 18u) Tx.maxValue dataSeries
            Expect.equal middle2 (set []) "middle2"
            let after = DataSeries.setGet (Date 20u) Tx.maxValue dataSeries
            Expect.equal after (set [13UL]) "after"
        }

        testProp "set vs map" (fun (first: Date * Tx * uint64,
                                    list: ((Date * Tx * uint64) * bool) list) ->
            let dataSeries =
                DataSeries.setAdd first 
                |> DataSeries.single
                |> List.foldBack (fun (datum, add) ->
                    let conv = if add then DataSeries.setAdd else DataSeries.setRemove
                    conv datum |> DataSeries.append
                ) list

            let fromList =
                let l =
                    (first,true) :: list
                    |> List.sortBy (fun ((d,t,v),a) -> d,t,v,a)
                fun date ->
                    List.fold (fun s ((d,_,v),a) ->
                        if d > date then s
                        elif a then Set.add v s
                        else Set.remove v s
                    ) Set.empty l
            
            let minDate = List.min (fst3 first :: List.map (fst >> fst3) list)
            if minDate > Date.minValue then
                let before = DataSeries.setGet (minDate-1) Tx.maxValue dataSeries
                Expect.equal before (set []) "before"

            fst3 first :: List.map (fst >> fst3) list
            |> List.distinct
            |> List.iter (fun eDate ->
                let actual = DataSeries.setGet eDate Tx.maxValue dataSeries
                let expectFromList = fromList eDate
                Expect.equal actual expectFromList "actual"

                let nextDate = eDate+1
                let next = DataSeries.setGet nextDate Tx.maxValue dataSeries
                let expectFromList = fromList nextDate
                Expect.equal next expectFromList "next"
            )
        )

        testProp "set tx query same as filtering first"
            (fun (first: Date * Tx * uint64,
                  list: (Date * Tx * uint64) list,
                  queryDate: Date,
                  queryTx: Tx) ->
            
            let dataSeriesFull =
                DataSeries.setAdd first
                |> DataSeries.single
                |> List.foldBack (DataSeries.setAdd >> DataSeries.append) list

            let dataSeriesFiltered =
                let filteredList =
                    first :: list
                    |> List.where (fun (_,tx,_) -> tx <= queryTx)
                match filteredList with
                | [] ->
                    List.minBy (fun (d,t,v) -> t,d,v) (first :: list)
                    |> DataSeries.setAdd
                    |> DataSeries.single
                | h::l ->
                    DataSeries.setAdd h
                    |> DataSeries.single
                    |> List.foldBack (DataSeries.setAdd >> DataSeries.append) l

            let full = DataSeries.setGet queryDate queryTx dataSeriesFull
            let filtered = DataSeries.setGet queryDate queryTx dataSeriesFiltered

            Expect.equal full filtered "same"
        )
    ]