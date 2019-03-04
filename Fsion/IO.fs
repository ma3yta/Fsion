namespace Fsion

[<NoEquality;NoComparison>]
type IO<'r,'a> =
    private
    | IOA of ('r -> 'a)
    static member Pure v =
        IOA v
    member m.Run(env:'r) =
        let (IOA run) = m
        run env
    member m.Bind(f:'a->IO<'r,'b>) =
        IOA (fun r -> (m.Run r |> f).Run r)

[<NoEquality;NoComparison>]
type IO<'r,'a,'e> =
    private
    | IOAE of ('r -> Result<'a,'e>)
    static member Pure v =
        IOAE v
    member m.Run(env:'r) =
        let (IOAE run) = m
        run env
    member m.Bind(f:'a->IO<'r,'b>) =
        IOAE (fun r ->
            match m.Run r with
            | Ok a -> (f a).Run r |> Ok
            | Error e -> Error e
        )
    member m.Bind(f:'a->IO<'r,'b,'e>) =
        IOAE (fun r ->
            match m.Run r with
            | Ok a -> (f a).Run r
            | Error e -> Error e
        )

type IO<'r,'a> with
    member m.Bind(f:'a->IO<'r,'b,'e>) =
        IOAE (fun r -> (m.Run r |> f).Run r)

type IOBuilder() =
    member inline __.Bind(io:IO<'r,'a>, f:'a -> IO<'r,'b>) : IO<'r,'b> = io.Bind f
    member inline __.Bind(io:IO<'r,'a,'e>, f:'a -> IO<'r,'b>) : IO<'r,'b,'e> = io.Bind f
    member inline __.Bind(io:IO<'r,'a>, f:'a -> IO<'r,'b,'e>) : IO<'r,'b,'e> = io.Bind f
    member inline __.Bind(io:IO<'r,'a,'e>, f:'a -> IO<'r,'b,'e>) = io.Bind f
    member inline __.Return value = IO<_,_>.Pure (fun _ -> value)
    member inline __.ReturnFrom value = value
    member inline __.Yield value = IO<_,_>.Pure (fun _ -> value)
    member inline __.Zero() = IO<_,_>.Pure (fun _ -> ())
    member inline __.Delay f = f()
    member inline __.For(xs:seq<'a>, f:'a -> IO<'r,'a,'e>) = Seq.map f xs
    member inline __.Run value = value
    member inline __.Effect(f:'r -> IO<'r,'a>) = IO<_,_>.Pure (fun r -> (f r).Run r)
    member inline __.Effect(f:'r -> IO<'r,'a,'e>) = IO<_,_,_>.Pure (fun r -> (f r).Run r)

[<AutoOpen>]
module IOAutoOpen =
    let io = IOBuilder()