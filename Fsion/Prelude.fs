namespace Fsion

open System

module VOption =
    let map f o =
        match o with
        | ValueSome i -> ValueSome (f i)
        | ValueNone -> ValueNone
    let get o =
        match o with
        | ValueSome i -> i
        | ValueNone -> failwith "ValueNone"
    let isSome o =
        match o with
        | ValueSome _ -> true
        | ValueNone -> false
    let isNone o =
        match o with
        | ValueSome _ -> false
        | ValueNone -> true
    let ofOption o =
        match o with
        | Some i -> ValueSome i
        | None -> ValueNone

module Result =
    let apply f x =
        match f,x with
        | Ok f, Ok v -> Ok (f v)
        | Error f, Ok _ -> Error f
        | Ok _, Error f -> Error [f]
        | Error f1, Error f2 -> Error (f2::f1)
    let ofOption format =
        let sb = Text.StringBuilder()
        Printf.kbprintf (fun () ->
            function
            | Some x -> Ok x
            | None -> sb.ToString() |> Error
        ) sb format
    let sequence list =
        List.fold (fun s i ->
            match s,i with
            | Ok l, Ok h -> Ok (h::l)
            | Error l, Ok _ -> Error l
            | Ok _, Error e -> Error [e]
            | Error l, Error h -> Error (h::l)
        ) (Ok []) list

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

[<Struct;Diagnostics.CodeAnalysis.SuppressMessage("NameConventions","TypeNamesMustBePascalCase")>]
type 'a list1 =
    private
    | List1 of 'a list
    member x.Length =
        let (List1 l) = x
        l.Length
    member x.Head =
        let (List1 l) = x
        List.head l
    member x.Tail =
        let (List1 l) = x
        List.tail l

module List1 =
    let cons x (List1 xs) = List1 (x::xs)
    let length (List1 l) = List.length l
    let head (List1 l) = List.head l
    let tail (List1 l) = List.tail l
    let init x xs = List1(x::xs)
    let tryOfList l =
        match l with
        | [] -> None
        | x -> List1 x |> Some
    let toList (List1 l) = l
    let toArray (List1 l) = List.toArray l
    let tryOfArray a =
        if Array.isEmpty a then None
        else List.ofArray a |> List1 |> Some
    let singleton s = List1 [s]
    let map mapper (List1 l) = List.map mapper l |> List1
    let iter action (List1 l) = List.iter action l
    let item index (List1 l) = List.item index l 
    let sort (List1 l) = List.sort l |> List1
    let append (List1 l1) (List1 l2) = List1(l1@l2)
    let choose chooser (List1 l) = List.choose chooser l |> tryOfList
    let fold folder state (List1 l) = List.fold folder state l
    let ofSeq source = List.ofSeq source |> tryOfList
    let forall predicate (List1 l) = List.forall predicate l


module Async =
    let sequential l =
        List.fold (fun s a ->
             async.Bind(a,fun t ->
                async {
                    let! l = s
                    return t::l
                }
            )
        ) (async.Return []) l