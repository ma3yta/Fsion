﻿module Fsion.Tests.SerializeTests

open System
open Expecto
open FsCheck
open Fsion

let serializeTests =

    let testRoundtrip serializeSet serializeGet i =
            let b,_ = serializeSet i Serialize.empty
            let j,_ = serializeGet (b,0)
            Expect.equal j i "testRoundtrip same"

    ftestList "serialize" [

        testAsync "zigzag examples" {
                Serialize.zigzag  0 |> Expect.equal 0u <| "0"
                Serialize.zigzag -1 |> Expect.equal 1u <| "-1"
                Serialize.zigzag  1 |> Expect.equal 2u <| "1"
                Serialize.zigzag -2 |> Expect.equal 3u <| "-2"
                Serialize.zigzag  2 |> Expect.equal 4u <| "2"
                Serialize.zigzag  Int32.MaxValue
                |> Expect.equal (UInt32.MaxValue-1u) <| "max-1"
                Serialize.zigzag  Int32.MinValue
                |> Expect.equal UInt32.MaxValue <| "max"
        }

        testAsync "unzigzag examples" {
                Serialize.unzigzag 0u |> Expect.equal  0 <| "0"
                Serialize.unzigzag 1u |> Expect.equal -1 <| "1"
                Serialize.unzigzag 2u |> Expect.equal  1 <| "2"
                Serialize.unzigzag 3u |> Expect.equal -2 <| "3"
                Serialize.unzigzag 4u |> Expect.equal  2 <| "4"
                Serialize.unzigzag (UInt32.MaxValue-1u)
                |> Expect.equal Int32.MaxValue <| "max-1"
                Serialize.unzigzag UInt32.MaxValue
                |> Expect.equal Int32.MinValue <| "max"
        }

        testProp "zigzag roundtrip" (fun (DoNotSize i) ->
            let j = Serialize.zigzag i |> Serialize.unzigzag
            Expect.equal j i "same"
        )

        testAsync "zigzag64 examples" {
                Serialize.zigzag64  0L |> Expect.equal 0UL <| "0"
                Serialize.zigzag64 -1L |> Expect.equal 1UL <| "-1"
                Serialize.zigzag64  1L |> Expect.equal 2UL <| "1"
                Serialize.zigzag64 -2L |> Expect.equal 3UL <| "-2"
                Serialize.zigzag64  2L |> Expect.equal 4UL <| "2"
                Serialize.zigzag64  Int64.MaxValue
                |> Expect.equal (UInt64.MaxValue-1UL) <| "max-1"
                Serialize.zigzag64 Int64.MinValue
                |> Expect.equal UInt64.MaxValue <| "max"
        }

        testAsync "unzigzag64 examples" {
                Serialize.unzigzag64 0UL |> Expect.equal  0L <| "0"
                Serialize.unzigzag64 1UL |> Expect.equal -1L <| "1"
                Serialize.unzigzag64 2UL |> Expect.equal  1L <| "2"
                Serialize.unzigzag64 3UL |> Expect.equal -2L <| "3"
                Serialize.unzigzag64 4UL |> Expect.equal  2L <| "4"
                Serialize.unzigzag64 (UInt64.MaxValue-1UL)
                |> Expect.equal Int64.MaxValue <| "max-1"
                Serialize.unzigzag64 UInt64.MaxValue
                |> Expect.equal Int64.MinValue <| "max"
        }

        testProp "zigzag64 roundtrip" (fun (DoNotSize i) ->
            let j = Serialize.zigzag64 i |> Serialize.unzigzag64
            Expect.equal j i "same"
        )

        testProp "bool roundtrip" (
            testRoundtrip Serialize.boolSet Serialize.boolGet
        )

        testProp "varint16 roundtrip" (fun (DoNotSize i) ->
            testRoundtrip Serialize.uint16Set Serialize.uint16Get i
        )

        testProp "varint32 roundtrip" (fun (DoNotSize i) ->
            testRoundtrip Serialize.uint32Set Serialize.uint32Get i
        )

        testProp "varint32 skip" (fun (DoNotSize i) ->
            let bs,i = Serialize.uint32Set i Serialize.empty
            let j = Serialize.uint32Skip (bs,0)
            Expect.equal i j "skip same"
        )

        testProp "varint64 roundtrip" (fun (DoNotSize i) ->
            testRoundtrip Serialize.uint64Set Serialize.uint64Get i
        )
    ]

let dataSeriesTests =

    ftestList "dataSeries" [
        
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
                |> DataSeries.add (Date 19u,Tx 29u,23L)
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
                |> DataSeries.add (Date 19u,Tx 29u,23L)
                |> DataSeries.add (Date 17u,Tx 37u,31L)
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
                |> List.foldBack DataSeries.add list

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
                |> List.foldBack DataSeries.add list

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
                    |> List.foldBack DataSeries.add l

            let full = DataSeries.get queryDate queryTx dataSeriesFull
            let filtered = DataSeries.get queryDate queryTx dataSeriesFiltered

            Expect.equal full filtered "same"
        )

        testAsync "one set get" {
            let dataSeries = DataSetSeries.single (Date 11u,Tx 17u,13UL)
            let exact = DataSetSeries.get (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact (set [13UL]) "exact"
            let before = DataSetSeries.get (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (set []) "before"
            let after = DataSetSeries.get (Date 12u) Tx.maxValue dataSeries
            Expect.equal after (set [13UL]) "after"
        }

        testAsync "two set get" {
            let dataSeries =
                DataSetSeries.single (Date 11u,Tx 17u,13UL)
                |> DataSetSeries.add (Date 19u,Tx 29u,23UL)
            let exact1 = DataSetSeries.get (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact1 (set [13UL]) "exact1"
            let exact2 = DataSetSeries.get (Date 19u) Tx.maxValue dataSeries
            Expect.equal exact2 (set [13UL;23UL]) "exact2"
            let before = DataSetSeries.get (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (set []) "before"
            let between = DataSetSeries.get (Date 15u) Tx.maxValue dataSeries
            Expect.equal between (set [13UL]) "between"
            let after = DataSetSeries.get (Date 20u) Tx.maxValue dataSeries
            Expect.equal after (set [13UL;23UL]) "after"
        }

        testAsync "three set get" {
            let dataSeries =
                DataSetSeries.single (Date 11u,Tx 17u,13UL)
                |> DataSetSeries.add (Date 19u,Tx 29u,23UL)
                |> DataSetSeries.add (Date 17u,Tx 37u,31UL)
            let exact1 = DataSetSeries.get (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact1 (set [13UL]) "exact1"
            let exact2 = DataSetSeries.get (Date 19u) Tx.maxValue dataSeries
            Expect.equal exact2 (set [13UL;23UL;31UL]) "exact2"
            let exact3 = DataSetSeries.get (Date 17u) Tx.maxValue dataSeries
            Expect.equal exact3 (set [13UL;31UL]) "exact3"
            let before = DataSetSeries.get (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (set []) "before"
            let middle1 = DataSetSeries.get (Date 15u) Tx.maxValue dataSeries
            Expect.equal middle1 (set [13UL]) "middle1"
            let middle2 = DataSetSeries.get (Date 18u) Tx.maxValue dataSeries
            Expect.equal middle2 (set [13UL;31UL]) "middle2"
            let after = DataSetSeries.get (Date 20u) Tx.maxValue dataSeries
            Expect.equal after (set [13UL;23UL;31UL]) "after"
        }

        testAsync "set get remove" {
            let dataSeries =
                DataSetSeries.single (Date 11u,Tx 17u,13UL)
                |> DataSetSeries.remove (Date 17u,Tx 29u,13UL)
                |> DataSetSeries.add (Date 19u,Tx 37u,13UL)
            let exact1 = DataSetSeries.get (Date 11u) Tx.maxValue dataSeries
            Expect.equal exact1 (set [13UL]) "exact1"
            let exact2 = DataSetSeries.get (Date 17u) Tx.maxValue dataSeries
            Expect.equal exact2 (set []) "exact2"
            let exact3 = DataSetSeries.get (Date 19u) Tx.maxValue dataSeries
            Expect.equal exact3 (set [13UL]) "exact3"
            let before = DataSetSeries.get (Date 10u) Tx.maxValue dataSeries
            Expect.equal before (set []) "before"
            let middle1 = DataSetSeries.get (Date 15u) Tx.maxValue dataSeries
            Expect.equal middle1 (set [13UL]) "middle1"
            let middle2 = DataSetSeries.get (Date 18u) Tx.maxValue dataSeries
            Expect.equal middle2 (set []) "middle2"
            let after = DataSetSeries.get (Date 20u) Tx.maxValue dataSeries
            Expect.equal after (set [13UL]) "after"
        }

        testProp "set vs map" (fun (first: Date * Tx * uint64,
                                    list: ((Date * Tx * uint64) * bool) list) ->
            let dataSeries =
                DataSetSeries.single first
                |> List.foldBack (fun (datum, add) ->
                    if add then DataSetSeries.add datum else DataSetSeries.remove datum
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
                let before = DataSetSeries.get (minDate-1) Tx.maxValue dataSeries
                Expect.equal before (set []) "before"

            fst3 first :: List.map (fst >> fst3) list
            |> List.distinct
            |> List.iter (fun eDate ->
                let actual = DataSetSeries.get eDate Tx.maxValue dataSeries
                let expectFromList = fromList eDate
                Expect.equal actual expectFromList "actual"

                let nextDate = eDate+1
                let next = DataSetSeries.get nextDate Tx.maxValue dataSeries
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
                DataSetSeries.single first
                |> List.foldBack DataSetSeries.add list

            let dataSeriesFiltered =
                let filteredList =
                    first :: list
                    |> List.where (fun (_,tx,_) -> tx <= queryTx)
                match filteredList with
                | [] ->
                    List.minBy (fun (d,t,v) -> t,d,v) (first :: list)
                    |> DataSetSeries.single
                | h::l ->
                    DataSetSeries.single h
                    |> List.foldBack DataSetSeries.add l

            let full = DataSetSeries.get queryDate queryTx dataSeriesFull
            let filtered = DataSetSeries.get queryDate queryTx dataSeriesFiltered

            Expect.equal full filtered "same"
        )
    ]