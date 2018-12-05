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

