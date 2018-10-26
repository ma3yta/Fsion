namespace Fsion

open System.Diagnostics.CodeAnalysis

[<AutoOpen>]
module Auto =
    let inline mapFst f (a,b) = f a,b
    let inline fst3 (i,_,_) = i
    let inline snd3 (_,i,_) = i
    let inline trd (_,_,i) = i

[<Struct;SuppressMessage("NameConventions","TypeNamesMustBePascalCase")>]
/// A non-empty list
type 'a list1 = private List1 of 'a list

module List1 =
    let head (List1 l) = List.head l
    let tail (List1 l) = List.tail l
    let init x xs = List1 (x::xs)
    let cons x (List1 xs) = List1 (x::xs)
    let tryOfList l = match l with | [] -> None | x::xs -> init x xs |> Some
    let toList (List1 l) = l
    let ofOne s = List1 [s]
    let map mapper (List1 l) = List.map mapper l |> List1
    let sort (List1 l) = List.sort l |> List1
    let fold folder state (List1 list) = List.fold folder state list
    let tryPick chooser (List1 list) = List.tryPick chooser list
    let tryChoose chooser (List1 list) =
        match List.choose chooser list with | [] -> None | l -> List1 l |> Some
    let tryCollect mapping (List1 list) =
        List.choose mapping list |> List.collect toList |> tryOfList
    let append (List1 l1) (List1 l2) = List1(l1@l2)

module File =
    open System.IO

    let getAll (path:string) includeSubdirectories =
        try
            let dirs = if includeSubdirectories then SearchOption.AllDirectories else SearchOption.TopDirectoryOnly
            Directory.GetFiles(path,"*",dirs) |> Ok
        with | e -> Error e

    let saveBytes filename bytes length =
        try
            use fs = File.Create filename
            fs.Write (bytes, 0, length)
            Ok ()
        with | e -> Error e

    let loadBytes filename =
        try
            use fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read)
            let rec load bs i l =
                let j = fs.Read (bs, i, l)
                if j=l then bs
                else load bs (i+j) (l-j)
            let l = int fs.Length
            load (Array.zeroCreate l) 0 l |> Ok
        with | e -> Error e