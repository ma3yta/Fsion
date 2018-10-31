namespace Fsion

open System
open System.IO
open Fsion

module internal ArraySerialize =

    let empty = [||],0

    let boolSet x (bs,i) =
        let bs = BytePool.ResizeUp bs (i+1)
        bs.[i] <- if x then 1uy else 0uy
        bs,i+1

    let boolGet (bs,i) =
        Array.get bs i = 1uy, i+1

    let uint16Set x (bs,i) =
        if x<0x80us then
            let bs = BytePool.ResizeUp bs (i+1)
            bs.[i] <- byte x
            bs,i+1
        elif x<0x4000us then
            let bs = BytePool.ResizeUp bs (i+2)
            bs.[i] <- byte (x>>>7) ||| 128uy
            bs.[i+1] <- byte x &&& 127uy
            bs,i+2
        else
            let bs = BytePool.ResizeUp bs (i+3)
            bs.[i] <- byte (x>>>14) ||| 128uy
            bs.[i+1] <- byte (x>>>7) ||| 128uy
            bs.[i+2] <- byte x &&& 127uy
            bs,i+3

    let uint16Get (bs,i) =
        let rec read i x =
            let b = Array.get bs i
            if b < 128uy then x+uint16 b, i+1
            else read (i+1) (x + uint16(b &&& 127uy) <<< 7)
        read i 0us

    let uint32Set x (bs,i) =
        if x<0x80u then
            let bs = BytePool.ResizeUp bs (i+1)
            bs.[i] <- byte x
            bs,i+1
        elif x<0x4000u then
            let bs = BytePool.ResizeUp bs (i+2)
            bs.[i] <- byte (x>>>7) ||| 128uy
            bs.[i+1] <- byte x &&& 127uy
            bs,i+2
        elif x<0x200000u then
            let bs = BytePool.ResizeUp bs (i+3)
            bs.[i] <- byte (x>>>14) ||| 128uy
            bs.[i+1] <- byte (x>>>7) ||| 128uy
            bs.[i+2] <- byte x &&& 127uy
            bs,i+3
        elif x<0x10000000u then
            let bs = BytePool.ResizeUp bs (i+4)
            bs.[i] <- byte (x>>>21) ||| 128uy
            bs.[i+1] <- byte (x>>>14) ||| 128uy
            bs.[i+2] <- byte (x>>>7) ||| 128uy
            bs.[i+3] <- byte x &&& 127uy
            bs,i+4
        else
            let bs = BytePool.ResizeUp bs (i+5)
            bs.[i] <- byte (x>>>28) ||| 128uy
            bs.[i+1] <- byte (x>>>21) ||| 128uy
            bs.[i+2] <- byte (x>>>14) ||| 128uy
            bs.[i+3] <- byte (x>>>7) ||| 128uy
            bs.[i+4] <- byte x &&& 127uy
            bs,i+5

    let uint32Get (bs,i) =
        let rec read i x =
            let b = Array.get bs i
            if b < 128uy then x+uint32 b, i+1
            else read (i+1) (x + uint32(b &&& 127uy) <<< 7)
        read i 0u

    let uint32Skip (bs,i) =
        let rec read i =
            if Array.get bs i < 128uy then i+1
            else read (i+1)
        read i

    let uint64Set x (bs,i) =
        if x<0x80UL then
            let bs = BytePool.ResizeUp bs (i+1)
            bs.[i] <- byte x
            bs,i+1
        elif x<0x4000UL then
            let bs = BytePool.ResizeUp bs (i+2)
            bs.[i] <- byte (x>>>7) ||| 128uy
            bs.[i+1] <- byte x &&& 127uy
            bs,i+2
        elif x<0x200000UL then
            let bs = BytePool.ResizeUp bs (i+3)
            bs.[i] <- byte (x>>>14) ||| 128uy
            bs.[i+1] <- byte (x>>>7) ||| 128uy
            bs.[i+2] <- byte x &&& 127uy
            bs,i+3
        elif x<0x10000000UL then
            let bs = BytePool.ResizeUp bs (i+4)
            bs.[i] <- byte (x>>>21) ||| 128uy
            bs.[i+1] <- byte (x>>>14) ||| 128uy
            bs.[i+2] <- byte (x>>>7) ||| 128uy
            bs.[i+3] <- byte x &&& 127uy
            bs,i+4
        elif x<0x800000000UL then
            let bs = BytePool.ResizeUp bs (i+5)
            bs.[i] <- byte (x>>>28) ||| 128uy
            bs.[i+1] <- byte (x>>>21) ||| 128uy
            bs.[i+2] <- byte (x>>>14) ||| 128uy
            bs.[i+3] <- byte (x>>>7) ||| 128uy
            bs.[i+4] <- byte x &&& 127uy
            bs,i+5
        elif x<0x40000000000UL then
            let bs = BytePool.ResizeUp bs (i+6)
            bs.[i] <- byte (x>>>35) ||| 128uy
            bs.[i+1] <- byte (x>>>28) ||| 128uy
            bs.[i+2] <- byte (x>>>21) ||| 128uy
            bs.[i+3] <- byte (x>>>14) ||| 128uy
            bs.[i+4] <- byte (x>>>7) ||| 128uy
            bs.[i+5] <- byte x &&& 127uy
            bs,i+6
        elif x<0x2000000000000UL then
            let bs = BytePool.ResizeUp bs (i+7)
            bs.[i] <- byte (x>>>42) ||| 128uy
            bs.[i+1] <- byte (x>>>35) ||| 128uy
            bs.[i+2] <- byte (x>>>28) ||| 128uy
            bs.[i+3] <- byte (x>>>21) ||| 128uy
            bs.[i+4] <- byte (x>>>14) ||| 128uy
            bs.[i+5] <- byte (x>>>7) ||| 128uy
            bs.[i+6] <- byte x &&& 127uy
            bs,i+7
        elif x<0x100000000000000UL then
            let bs = BytePool.ResizeUp bs (i+8)
            bs.[i] <- byte (x>>>49) ||| 128uy
            bs.[i+1] <- byte (x>>>42) ||| 128uy
            bs.[i+2] <- byte (x>>>35) ||| 128uy
            bs.[i+3] <- byte (x>>>28) ||| 128uy
            bs.[i+4] <- byte (x>>>21) ||| 128uy
            bs.[i+5] <- byte (x>>>14) ||| 128uy
            bs.[i+6] <- byte (x>>>7) ||| 128uy
            bs.[i+7] <- byte x &&& 127uy
            bs,i+8
        elif x<0x8000000000000000UL then
            let bs = BytePool.ResizeUp bs (i+9)
            bs.[i] <- byte (x>>>56) ||| 128uy
            bs.[i+1] <- byte (x>>>49) ||| 128uy
            bs.[i+2] <- byte (x>>>42) ||| 128uy
            bs.[i+3] <- byte (x>>>35) ||| 128uy
            bs.[i+4] <- byte (x>>>28) ||| 128uy
            bs.[i+5] <- byte (x>>>21) ||| 128uy
            bs.[i+6] <- byte (x>>>14) ||| 128uy
            bs.[i+7] <- byte (x>>>7) ||| 128uy
            bs.[i+8] <- byte x &&& 127uy
            bs,i+9
        else
            let bs = BytePool.ResizeUp bs (i+10)
            bs.[i] <- byte (x>>>63) ||| 128uy
            bs.[i+1] <- byte (x>>>56) ||| 128uy
            bs.[i+2] <- byte (x>>>49) ||| 128uy
            bs.[i+3] <- byte (x>>>42) ||| 128uy
            bs.[i+4] <- byte (x>>>35) ||| 128uy
            bs.[i+5] <- byte (x>>>28) ||| 128uy
            bs.[i+6] <- byte (x>>>21) ||| 128uy
            bs.[i+7] <- byte (x>>>14) ||| 128uy
            bs.[i+8] <- byte (x>>>7) ||| 128uy
            bs.[i+9] <- byte x &&& 127uy
            bs,i+10

    let uint64Get (bs,i) =
        let rec read i x =
            let b = Array.get bs i
            if b < 128uy then x+uint64 b, i+1
            else read (i+1) (x + uint64(b &&& 127uy) <<< 7)
        read i 0uL

    let textSet (Text s) (bs,i) =
        let b = System.Text.UTF8Encoding.UTF8.GetBytes s
        let l = Array.length b
        let bs,i = uint32Set (uint32 l) (bs,i)
        let bs = BytePool.ResizeUp bs (i+l)
        Buffer.BlockCopy(b, 0, bs, i, l)
        bs,i+l
        
    let textGet (bs,i) =
        let l,i = uint32Get (bs,i)
        let l = int l
        System.Text.UTF8Encoding.UTF8.GetString(bs, i, l) |> Text,i+l

    let entityTypeSet (EntityType eid) (bs,i) =
        uint32Set eid (bs,i)


    let entityTypeGet (bs,i) =
        let eid,i = uint32Get (bs,i)
        EntityType eid, i


    let entitySet (Entity(et,eid)) (bs,i) =
        entityTypeSet et (bs,i)
        |> uint32Set eid

    let entityGet (bs,i) =
        let et,i = entityTypeGet (bs,i)
        let eid,i = uint32Get (bs,i)
        Entity(et,eid), i

    let attributeSet (Attribute aid) (bs,i) =
        uint32Set aid (bs,i)

    let attributeGet (bs,i) =
        let eid,i = uint32Get (bs,i)
        Attribute eid, i


[<Struct>]
type DataSeries = 
    internal
    | DataSeries of byte []

open System.Collections.Generic

module internal DataSeries =
    open ArraySerialize
    
    /// Create a new DataSetSeries from a single datum.
    let single (Date dt,Tx tx,value) =
        empty
        |> uint32Set dt
        |> uint32Set tx
        |> uint64Set (zigzag64 value)
        |> BytePool.ResizeExact
        |> DataSeries

    /// Create a new DataSetSeries from a datum and DataSetSeries.
    let append (Date newDate,Tx newTx, newValue) (DataSeries dataSeries) =
        let currentDate,i = uint32Get (dataSeries,0)
        let currentTx,i = uint32Get (dataSeries,i)
        let currentValue,i = uint64Get (dataSeries,i) |> mapFst unzigzag64
        match compare (newDate, newTx, newValue) (currentDate, currentTx, currentValue) with
        | 0 -> DataSeries dataSeries
        | 1 ->
            let bs,j =
                empty
                |> uint32Set newDate
                |> uint32Set newTx
                |> uint64Set (zigzag64 newValue)
                |> uint32Set (newDate - currentDate)
                |> uint32Set (zigzag (int(newTx - currentTx)))
                |> uint64Set (zigzag64 (newValue - currentValue))
            let nDataSeries = Array.zeroCreate (Array.length dataSeries+j-i)
            Array.Copy(bs, nDataSeries, j)
            BytePool.Return bs
            if Array.length dataSeries <> i then
                Array.Copy(dataSeries, i, nDataSeries, j, Array.length dataSeries-i)
            DataSeries nDataSeries
        | _ ->
            let rec getValue i currentDate currentValue currentTx =
                if i = Array.length dataSeries then
                    let bs,j =
                        empty
                        |> uint32Set (currentDate - newDate)
                        |> uint32Set (zigzag (int(currentTx - newTx)))
                        |> uint64Set (zigzag64 (currentValue - newValue))
                    let nDataSeries = Array.zeroCreate (i+j)
                    Array.Copy(bs, 0, nDataSeries, i, j)
                    BytePool.Return bs
                    Array.Copy(dataSeries, nDataSeries, i)
                    DataSeries nDataSeries
                else
                    let dd,j = uint32Get (dataSeries,i)
                    let dt,j = uint32Get (dataSeries,j) |> mapFst (unzigzag >> uint32)
                    let dv,j = uint64Get (dataSeries,j) |> mapFst unzigzag64
                    match compare (newDate, newTx, newValue)
                                  (currentDate-dd, currentTx-dt, currentValue-dv) with
                    | 0 -> DataSeries dataSeries
                    | 1 ->
                        let bs,k =
                            empty
                            |> uint32Set (currentDate - newDate)
                            |> uint32Set (zigzag (int(currentTx - newTx)))
                            |> uint64Set (zigzag64 (currentValue - newValue))
                            |> uint32Set (dd + newDate - currentDate)
                            |> uint32Set (zigzag (int(dt + newTx - currentTx)))
                            |> uint64Set (zigzag64 (dv + newValue - currentValue))
                        let nDataSeries =
                            Array.zeroCreate (i+k+Array.length dataSeries-j)
                        Array.Copy(bs, 0, nDataSeries, i, k)
                        BytePool.Return bs
                        Array.Copy(dataSeries, nDataSeries, i)
                        Array.Copy(dataSeries, j, nDataSeries, i+k,
                            Array.length dataSeries-j)
                        DataSeries nDataSeries
                    | _ ->
                        getValue j (currentDate-dd) (currentValue-dv) (currentTx-dt)
            getValue i currentDate currentValue currentTx

    /// Returns the closest datum from a DataSetSeries for a queryDate and queryTx.
    let get (Date queryDate) (Tx queryTx) (DataSeries dataSeries) =
        let currentDate,i = uint32Get (dataSeries,0)
        let currentTx,i = uint32Get (dataSeries,i)
        let currentValue,i = uint64Get (dataSeries,i) |> mapFst unzigzag64
        let rec getValue i currentDate currentValue currentTx (bestDate,bestValue,bestTx) =
            if queryDate >= currentDate && queryTx >= currentTx then
                Date currentDate, Tx currentTx, currentValue
            elif i = Array.length dataSeries then
                Date bestDate, Tx bestTx, bestValue
            else
                let dd,i = uint32Get (dataSeries,i)
                let dt,i = uint32Get (dataSeries,i) |> mapFst (unzigzag >> uint32)
                let dv,i = uint64Get (dataSeries,i) |> mapFst unzigzag64
                let nextDate = currentDate - dd
                let nextValue = currentValue - dv
                let nextTx = currentTx - dt
                getValue i nextDate nextValue nextTx
                    (if (bestTx >= nextTx && bestTx > queryTx)
                        || (queryTx >= nextTx && nextDate < bestDate)
                     then nextDate, nextValue, nextTx
                     else bestDate, bestValue, bestTx)
        getValue i currentDate currentValue currentTx
            (currentDate,currentValue,currentTx)

    /// Create a new datum from an add set datum.
    let setAdd (date,tx,newValue:uint64) : Datum =
        date,tx,int64 newValue

    /// Create a new DataSetSeries from a remove set datum and DataSetSeries.
    let setRemove (date,tx,newValue:uint64) : Datum =
        date,tx,~~~(int64 newValue)

    /// Returns the closest data set from a DataSetSeries for a queryDate and query transaction.
    let setGet (Date queryDate) (Tx queryTx) (DataSeries dataSeries) =
        let currentDate,i = uint32Get (dataSeries,0)
        let currentTx,i = uint32Get (dataSeries,i)
        let currentValue,i = uint64Get (dataSeries,i) |> mapFst unzigzag64
        let removed = HashSet()
        let mutable added = Set.empty
        let rec getValue i currentDate currentValue currentTx =
            if queryDate >= currentDate && queryTx >= currentTx then
                if currentValue < 0L then
                    removed.Add ~~~currentValue |> ignore
                elif removed.Contains currentValue |> not then
                    added <- Set.add (uint64 currentValue) added
            if i = Array.length dataSeries then added
            else
                let dd,i = uint32Get (dataSeries,i)
                let dt,i = uint32Get (dataSeries,i) |> mapFst (unzigzag >> uint32)
                let dv,i = uint64Get (dataSeries,i) |> mapFst unzigzag64
                let nextDate = currentDate - dd
                let nextValue = currentValue - dv
                let nextTx = currentTx - dt
                getValue i nextDate nextValue nextTx
        getValue i currentDate currentValue currentTx

    

module StreamSerialize =
    
    let rec private read (s:Stream) bs offset count =
        let bytesRead = s.Read(bs, offset, count)
        if bytesRead<>count then read s bs (offset+bytesRead) (count-bytesRead)

    let uint32Set (s:Stream) u =
        let bs,i = ArraySerialize.uint32Set u ArraySerialize.empty
        s.Write(bs, 0, i)
        BytePool.Return bs

    let uint32Get (s:Stream) =
        let rec read x =
            let b = s.ReadByte()
            if b < 128 then x+uint32 b
            else read (x + uint32(byte b &&& 127uy) <<< 7)
        read 0u

    let uint64Set (s:Stream) u =
        let bs,i = ArraySerialize.uint64Set u ArraySerialize.empty
        s.Write(bs, 0, i)
        BytePool.Return bs
    
    let uint64Get (s:Stream) =
        let rec read x =
            let b = s.ReadByte() |> byte
            if b < 128uy then x+uint64 b
            else read (x + uint64(b &&& 127uy) <<< 7)
        read 0uL

    let textSet (s:Stream) t =
        let bs,i = ArraySerialize.textSet t ArraySerialize.empty
        s.Write(bs, 0, i)
        BytePool.Return bs

    let textGet (s:Stream) =
        let l = uint32Get s |> int
        let bs = BytePool.Rent l
        read s bs 0 l
        let t = System.Text.UTF8Encoding.UTF8.GetString(bs, 0, l) |> Text
        BytePool.Return bs
        t

    let bytesSet (s:Stream) bs =
        uint32Set s (Array.length bs |> uint32)
        s.Write(bs, 0, bs.Length)

    let bytesGet (s:Stream) =
        let l = uint32Get s |> int
        let bs = Array.zeroCreate l
        read s bs 0 l
        bs

    let textListSet (s:Stream) (l:Text ResizeArray) =
        uint32Set s (uint32 l.Count)
        Seq.iter (textSet s) l

    let textListLoad (s:Stream) (a:Text ResizeArray) =
        let l = uint32Get s |> int
        a.Clear()
        let rec repeat i =
            if i<>0 then
                textGet s |> a.Add
                repeat (i-1)
        repeat l

    let byteListSet (s:Stream) (l:byte[] ResizeArray) =
        uint32Set s (uint32 l.Count)
        Seq.iter (bytesSet s) l

    let byteListLoad (s:Stream) (a:byte[] ResizeArray) =
        let l = uint32Get s |> int
        a.Clear()
        let rec repeat i =
            if i<>0 then
                bytesGet s |> a.Add
                repeat (i-1)
        repeat l

    let entityTypeSet (s:Stream) (EntityType et) =
        uint32Set s et

    let entityTypeGet (s:Stream) =
        uint32Get s |> EntityType

    let entitySet (s:Stream) (Entity(et,eid)) =
        entityTypeSet s et
        uint32Set s eid

    let entityGet (s:Stream) =
        let et = entityTypeGet s
        let eid = uint32Get s
        Entity(et,eid)

    let attributeSet (s:Stream) (Attribute aid) =
        uint32Set s aid

    let attributeGet (s:Stream) =
        let aid = uint32Get s
        Attribute aid

    let entityAttributeSet (s:Stream) ((entity,attribute):Entity * Attribute) =
        let bs,i =
            ArraySerialize.entitySet entity ArraySerialize.empty
            |> ArraySerialize.attributeSet attribute
        s.Write(bs, 0, i)
        BytePool.Return bs

    let entityAttributeGet (s:Stream) =
        entityGet s, attributeGet s

    let dataSeriesDictionarySet (s:Stream) (dictionary:Dictionary<_,_>) =
        uint32Set s (uint32 dictionary.Count)
        dictionary |> Seq.iter (fun kv ->
            entityAttributeSet s kv.Key
            let (DataSeries bytes) = kv.Value
            bytesSet s bytes
        )

    let dataSeriesDictionaryLoad (s:Stream) (d:Dictionary<_,_>) =
        let l = uint32Get s |> int
        d.Clear()
        for i = 0 to l-1 do
            d.Add(entityAttributeGet s, bytesGet s |> DataSeries)

    let transactionDataSet (s:Stream) (transactionData:TransactionData) =
        
        let headers = transactionData.Headers
        uint32Set s (uint32 headers.Length)
        List.iter (fun (Attribute a,d) ->
            uint32Set s a
            uint64Set s (zigzag64 d)
        ) headers
        
        let creates = transactionData.Creates
        uint32Set s (uint32 creates.Length)
        List.fold (fun (pet,peid,pa,pd,pv) (Entity(EntityType et,eid),Attribute a,Date d,v) ->
            uint32Set s (et-pet)
            uint32Set s (eid-peid)
            uint32Set s (a-pa)
            uint32Set s (d-pd)
            let v = zigzag64 v
            uint64Set s (v-pv)
            (et,eid,a,d,v)
        ) (0u,0u,0u,0u,0uL) creates |> ignore
        
        let updates = transactionData.Updates
        uint32Set s (uint32 updates.Length)
        List.fold (fun (pet,peid,pa,pd,pv) (Entity(EntityType et,eid),Attribute a,Date d,v) ->
            uint32Set s (et-pet)
            uint32Set s (eid-peid)
            uint32Set s (a-pa)
            uint32Set s (d-pd)
            let v = zigzag64 v
            uint64Set s (v-pv)
            (et,eid,a,d,v)
        ) (0u,0u,0u,0u,0uL) updates |> ignore

        let text = transactionData.Text
        uint32Set s (uint32 text.Length)
        Array.iter (textSet s) text

        let data = transactionData.Data
        uint32Set s (uint32 data.Length)
        Array.iter (bytesSet s) data

    let transactionDataGet (s:Stream) =
        let datumList() =
            let l = uint32Get s |> int
            Seq.init l (fun _ -> uint32Get s, uint32Get s, uint32Get s, uint32Get s, uint64Get s)
            |> Seq.scan (fun (pet,peid,pa,pd,pv) (et,eid,a,d,v) -> pet+et,peid+eid,pa+a,pd+d,pv+v) (0u,0u,0u,0u,0uL)
            |> Seq.tail
            |> Seq.map (fun (et,eid,a,d,v) -> Entity(EntityType et,eid), Attribute a, Date d, unzigzag64 v)
            |> List.ofSeq
        {
            Headers =
                let l = uint32Get s |> int
                List.init l (fun _ ->
                    attributeGet s, (uint64Get s |> unzigzag64)
                )
            Creates = datumList()
            Updates = datumList()
            Text =
                let l = uint32Get s |> int
                Array.init l (fun _ -> textGet s)
            Data =
                let l = uint32Get s |> int
                Array.init l (fun _ -> bytesGet s)
        }