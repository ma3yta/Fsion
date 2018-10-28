namespace Fsion

open System
open System.IO
open System.Buffers
open Fsion

module internal ArraySerialize =
 
    let zigzag i = (i <<< 1) ^^^ (i >>> 31) |> uint32
    let unzigzag i = int(i >>> 1) ^^^ -int(i &&& 1u)
    let zigzag64 (i:int64) = (i <<< 1) ^^^ (i >>> 63) |> uint64
    let unzigzag64 i = int64(i >>> 1) ^^^ -int64(i &&& 1UL)
    
    let arrayPool = ArrayPool<byte>.Shared
    
    let private resizeUp bytes i =
        let l = Array.length bytes
        let rec doubleUp j =
            if j>=i then j
            else doubleUp (j<<<1)
        if l>=i then bytes
        elif l=0 then doubleUp 16 |> arrayPool.Rent
        else
            let newBytes = doubleUp (l<<<1) |> arrayPool.Rent
            Buffer.BlockCopy(bytes, 0, newBytes, 0, l)
            arrayPool.Return bytes
            newBytes

    let resizeDown (bs,i) =
        if Array.length bs = i then bs
        else
            let nbs = Array.zeroCreate i
            Array.Copy(bs,nbs,i)
            arrayPool.Return bs
            nbs

    let empty = [||],0

    let boolSet x (bs,i) =
        let bs = resizeUp bs (i+1)
        bs.[i] <- if x then 1uy else 0uy
        bs,i+1

    let boolGet (bs,i) =
        Array.get bs i = 1uy, i+1

    let uint16Set x (bs,i) =
        if x<0x80us then
            let bs = resizeUp bs (i+1)
            bs.[i] <- byte x
            bs,i+1
        elif x<0x4000us then
            let bs = resizeUp bs (i+2)
            bs.[i] <- byte (x>>>7) ||| 128uy
            bs.[i+1] <- byte x &&& 127uy
            bs,i+2
        else
            let bs = resizeUp bs (i+3)
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
            let bs = resizeUp bs (i+1)
            bs.[i] <- byte x
            bs,i+1
        elif x<0x4000u then
            let bs = resizeUp bs (i+2)
            bs.[i] <- byte (x>>>7) ||| 128uy
            bs.[i+1] <- byte x &&& 127uy
            bs,i+2
        elif x<0x200000u then
            let bs = resizeUp bs (i+3)
            bs.[i] <- byte (x>>>14) ||| 128uy
            bs.[i+1] <- byte (x>>>7) ||| 128uy
            bs.[i+2] <- byte x &&& 127uy
            bs,i+3
        elif x<0x10000000u then
            let bs = resizeUp bs (i+4)
            bs.[i] <- byte (x>>>21) ||| 128uy
            bs.[i+1] <- byte (x>>>14) ||| 128uy
            bs.[i+2] <- byte (x>>>7) ||| 128uy
            bs.[i+3] <- byte x &&& 127uy
            bs,i+4
        else
            let bs = resizeUp bs (i+5)
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
            let bs = resizeUp bs (i+1)
            bs.[i] <- byte x
            bs,i+1
        elif x<0x4000UL then
            let bs = resizeUp bs (i+2)
            bs.[i] <- byte (x>>>7) ||| 128uy
            bs.[i+1] <- byte x &&& 127uy
            bs,i+2
        elif x<0x200000UL then
            let bs = resizeUp bs (i+3)
            bs.[i] <- byte (x>>>14) ||| 128uy
            bs.[i+1] <- byte (x>>>7) ||| 128uy
            bs.[i+2] <- byte x &&& 127uy
            bs,i+3
        elif x<0x10000000UL then
            let bs = resizeUp bs (i+4)
            bs.[i] <- byte (x>>>21) ||| 128uy
            bs.[i+1] <- byte (x>>>14) ||| 128uy
            bs.[i+2] <- byte (x>>>7) ||| 128uy
            bs.[i+3] <- byte x &&& 127uy
            bs,i+4
        elif x<0x800000000UL then
            let bs = resizeUp bs (i+5)
            bs.[i] <- byte (x>>>28) ||| 128uy
            bs.[i+1] <- byte (x>>>21) ||| 128uy
            bs.[i+2] <- byte (x>>>14) ||| 128uy
            bs.[i+3] <- byte (x>>>7) ||| 128uy
            bs.[i+4] <- byte x &&& 127uy
            bs,i+5
        elif x<0x40000000000UL then
            let bs = resizeUp bs (i+6)
            bs.[i] <- byte (x>>>35) ||| 128uy
            bs.[i+1] <- byte (x>>>28) ||| 128uy
            bs.[i+2] <- byte (x>>>21) ||| 128uy
            bs.[i+3] <- byte (x>>>14) ||| 128uy
            bs.[i+4] <- byte (x>>>7) ||| 128uy
            bs.[i+5] <- byte x &&& 127uy
            bs,i+6
        elif x<0x2000000000000UL then
            let bs = resizeUp bs (i+7)
            bs.[i] <- byte (x>>>42) ||| 128uy
            bs.[i+1] <- byte (x>>>35) ||| 128uy
            bs.[i+2] <- byte (x>>>28) ||| 128uy
            bs.[i+3] <- byte (x>>>21) ||| 128uy
            bs.[i+4] <- byte (x>>>14) ||| 128uy
            bs.[i+5] <- byte (x>>>7) ||| 128uy
            bs.[i+6] <- byte x &&& 127uy
            bs,i+7
        elif x<0x100000000000000UL then
            let bs = resizeUp bs (i+8)
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
            let bs = resizeUp bs (i+9)
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
            let bs = resizeUp bs (i+10)
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
        let bs = resizeUp bs (i+l)
        Buffer.BlockCopy(b, 0, bs, i, l)
        bs,i+l
        
    let textGet (bs,i) =
        let l,i = uint32Get (bs,i)
        let l = int l
        System.Text.UTF8Encoding.UTF8.GetString(bs, i, l) |> Text,i+l

    let entityTypeSet (EntityType b) (bs,i) =
        let bs = resizeUp bs (i+1)
        bs.[i] <- b
        bs,i+1

    let entityTypeGet (bs,i) =
        Array.get bs i |> EntityType, i+1

    let entitySet (Entity(et,eid)) (bs,i) =
        entityTypeSet et (bs,i)
        |> uint32Set eid

    let entityGet (bs,i) =
        let et,i = entityTypeGet (bs,i)
        let eid,i = uint32Get (bs,i)
        Entity(et,eid), i

    let attributeSet (a:Attribute) (bs,i) =
        uint32Set a.Id (bs,i)

    let attributeGet (getAttribute:uint32->Attribute) (bs,i) =
        let eid,i = uint32Get (bs,i)
        getAttribute eid, i


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
        |> resizeDown
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
            arrayPool.Return bs
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
                    arrayPool.Return bs
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
                        arrayPool.Return bs
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

    let uint32Set u (s:Stream) =
        let bs,i = ArraySerialize.uint32Set u ArraySerialize.empty
        s.Write(bs, 0, i)
        ArraySerialize.arrayPool.Return bs

    let uint32Get (s:Stream) =
        let rec read x =
            let b = s.ReadByte()
            if b < 128 then x+uint32 b
            else read (x + uint32(byte b &&& 127uy) <<< 7)
        read 0u
    
    let textSet (s:Stream) t =
        let bs,i = ArraySerialize.textSet t ArraySerialize.empty
        s.Write(bs, 0, i)
        ArraySerialize.arrayPool.Return bs

    let textGet (s:Stream) =
        let l = uint32Get s |> int
        let bs = ArraySerialize.arrayPool.Rent l
        read s bs 0 l
        let t = System.Text.UTF8Encoding.UTF8.GetString(bs, 0, l) |> Text
        ArraySerialize.arrayPool.Return bs
        t

    let textListSet (l:Text ResizeArray) (s:Stream) =
        uint32Set (uint32 l.Count) s
        Seq.iter (textSet s) l

    let textListLoad (s:Stream) (a:Text ResizeArray) =
        let l = uint32Get s |> int
        a.Clear()
        for i = 0 to l-1 do
            a.[i] <- textGet s

    let bytesSet (s:Stream) bs =
        uint32Set (Array.length bs |> uint32) s
        s.Write(bs, 0, bs.Length)

    let bytesGet (s:Stream) =
        let l = uint32Get s |> int
        let bs = Array.zeroCreate l
        read s bs 0 l
        bs

    let byteListSet (l:byte[] ResizeArray) (s:Stream) =
        uint32Set (uint32 l.Count) s
        Seq.iter (bytesSet s) l

    let byteListLoad (s:Stream) (a:byte[] ResizeArray) =
        let l = uint32Get s |> int
        a.Clear()
        for i = 0 to l-1 do
            a.[i] <- bytesGet s

    let entityTypeGet (s:Stream) =
        s.ReadByte() |> byte |> EntityType
    
    let entityGet (s:Stream) =
        let et = entityTypeGet s
        let eid = uint32Get s
        Entity(et,eid)

    let attributeGet (getAttribute:uint32->Attribute) (s:Stream) =
        let eid = uint32Get s
        getAttribute eid

    let entityAttributeSet ((entity,attribute):Entity * Attribute) (s:Stream) =
        let bs,i =
            ArraySerialize.entitySet entity ArraySerialize.empty
            |> ArraySerialize.attributeSet attribute
        s.Write(bs, 0, i)
        ArraySerialize.arrayPool.Return bs

    let entityAttributeGet (getAttribute:uint32->Attribute) (s:Stream) =
        entityGet s, attributeGet getAttribute s

    let dataSeriesDictionarySet (dictionary:Dictionary<_,_>) (s:Stream) =
        uint32Set (uint32 dictionary.Count) s
        dictionary |> Seq.iter (fun kv ->
            entityAttributeSet kv.Key s
            let (DataSeries bytes) = kv.Value
            bytesSet s bytes
        )

    let dataSeriesDictionaryLoad (getAttribute:uint32->Attribute) (d:Dictionary<_,_>) (s:Stream) =
        let l = uint32Get s |> int
        d.Clear()
        for i = 0 to l-1 do
            d.Add(entityAttributeGet getAttribute s, bytesGet s |> DataSeries)