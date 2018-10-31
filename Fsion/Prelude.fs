namespace Fsion

open System
open System.Threading

[<AutoOpen>]
module Auto =
    let inline mapFst f (a,b) = f a,b
    let inline fst3 (i,_,_) = i
    let inline snd3 (_,i,_) = i
    let inline trd (_,_,i) = i
    let inline zigzag (i:int) = (i <<< 1) ^^^ (i >>> 31) |> uint32
    let inline unzigzag (i:uint32) = int(i >>> 1) ^^^ -int(i &&& 1u)
    let inline zigzag64 (i:int64) = (i <<< 1) ^^^ (i >>> 63) |> uint64
    let inline unzigzag64 (i:uint64) = int64(i >>> 1) ^^^ -int64(i &&& 1UL)

//open System.Diagnostics.CodeAnalysis

//[<Struct;SuppressMessage("NameConventions","TypeNamesMustBePascalCase")>]
///// A non-empty list
//type 'a list1 = private List1 of 'a list

//module List1 =
//    let head (List1 l) = List.head l
//    let tail (List1 l) = List.tail l
//    let init x xs = List1 (x::xs)
//    let cons x (List1 xs) = List1 (x::xs)
//    let tryOfList l = match l with | [] -> None | x::xs -> init x xs |> Some
//    let toList (List1 l) = l
//    let ofOne s = List1 [s]
//    let map mapper (List1 l) = List.map mapper l |> List1
//    let sort (List1 l) = List.sort l |> List1
//    let fold folder state (List1 list) = List.fold folder state list
//    let tryPick chooser (List1 list) = List.tryPick chooser list
//    let tryChoose chooser (List1 list) =
//        match List.choose chooser list with | [] -> None | l -> List1 l |> Some
//    let tryCollect mapping (List1 list) =
//        List.choose mapping list |> List.collect toList |> tryOfList
//    let append (List1 l1) (List1 l2) = List1(l1@l2)

module internal File =
    open System.IO

    let list (path:string) (pattern:string) =
        try
            Directory.GetFiles(path, pattern,
                SearchOption.TopDirectoryOnly) |> Ok
        with e -> Error e

    let listAllDirectories (path:string) (pattern:string) =
        try
            Directory.GetFiles(path, pattern,
                SearchOption.AllDirectories) |> Ok
        with e -> Error e

    let saveBytes filename bytes length =
        try
            use fs = File.Create filename
            fs.Write (bytes, 0, length)
            Ok ()
        with e -> Error e

    let loadBytes filename =
        try
            use fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read)
            let rec load bs i l =
                let j = fs.Read (bs, i, l)
                if j=l then bs
                else load bs (i+j) (l-j)
            let l = int fs.Length
            load (Array.zeroCreate l) 0 l |> Ok
        with e -> Error e

type private BytePoolBucket = {
    mutable Count: int
    mutable Lock: SpinLock
    Buffer: byte[][]
}
type BytePool private () =
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
        bucket.Lock.Enter lockTaken
        try
            if bucket.Count = 0 then
                Array.zeroCreate (128 <<< bucketIndex)
            else
                bucket.Count <- bucket.Count - 1
                let bs = bucket.Buffer.[bucket.Count]
                bucket.Buffer.[bucket.Count] <- null
                bs
        finally
            if !lockTaken then bucket.Lock.Exit false
    static member Return (bytes:byte[]) =
        let bucketIndex = bucketIndex bytes.Length 0
        let bucket = buckets.[bucketIndex]
        let lockTaken = ref false
        bucket.Lock.Enter lockTaken
        try
            if bucket.Count < bucket.Buffer.Length then
                bucket.Buffer.[bucket.Count] <- bytes
                bucket.Count <- bucket.Count + 1
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