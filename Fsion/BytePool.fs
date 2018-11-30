namespace Fsion

open System
open System.Threading

[<NoComparison;NoEquality>]
type private BytePoolBucket = {
    mutable Count: int
    mutable Lock: SpinLock
    Buffer: byte[][]
}
type internal BytePool private () =
    static let rec bucketIndex i j =
        if i <= 128 then j
        else bucketIndex (i>>>1) (j+1)
    static let buckets =
        Array.init 16 (fun _ ->
            {
                Count = 0
                Lock = SpinLock false
                Buffer = Array.zeroCreate Environment.ProcessorCount
            }
        )
    static member Rent i =
        let bucketIndex = bucketIndex i 0
        let bucket = buckets.[bucketIndex]
        let lockTaken = ref false
        let mutable bs = Unchecked.defaultof<_>
        bucket.Lock.Enter lockTaken
        try
            if bucket.Count < bucket.Buffer.Length then
                bs <- bucket.Buffer.[bucket.Count]
                bucket.Buffer.[bucket.Count] <- null
                bucket.Count <- bucket.Count + 1
        finally
            if !lockTaken then bucket.Lock.Exit false
        if isNull bs then Array.zeroCreate (128 <<< bucketIndex)
        else bs
    static member Return (bytes:byte[]) =
        let bucketIndex = bucketIndex bytes.Length 0
        let bucket = buckets.[bucketIndex]
        let lockTaken = ref false
        bucket.Lock.Enter lockTaken
        try
            if bucket.Count <> 0 then
                bucket.Count <- bucket.Count - 1
                bucket.Buffer.[bucket.Count] <- bytes
        finally
            if !lockTaken then bucket.Lock.Exit false
    static member ResizeUp bytes i =
        let l = Array.length bytes
        if l>=i then bytes
        elif l=0 then BytePool.Rent 128
        else
            let rec doubleUp l =
                let l = l<<<1
                if l>=i then l else doubleUp l
            let newBytes = doubleUp l |> BytePool.Rent
            Buffer.BlockCopy(bytes, 0, newBytes, 0, l)
            BytePool.Return bytes
            newBytes
    static member ResizeExact (bytes,i) =
        if Array.length bytes = i then bytes
        else
            let newBytes = Array.zeroCreate i
            Array.Copy(bytes,newBytes,i)
            BytePool.Return bytes
            newBytes

