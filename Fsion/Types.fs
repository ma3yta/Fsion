namespace Fsion

open System

[<Struct>]
type Text =
    internal
    | Text of string
    static member (+)(Text t1,Text t2) = Text(t1+t2)
    static member (+)(s:string,Text t) = Text(s+t)
    static member (+)(Text t,s:string) = Text(t+s)

module Text =
    let ofString s =
        if String.IsNullOrWhiteSpace s then None
        else s.Trim() |> Text |> Some
    let toString (Text s) = s
    let length (Text s) = s.Length

type ResizeMap<'a,'b> = Collections.Generic.Dictionary<'a,'b>

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
            | None -> sb.ToString() |> Text |> Error
        ) sb format
    let sequence list =
        List.fold (fun s i ->
            match s,i with
            | Ok l, Ok h -> Ok (h::l)
            | Error l, Ok _ -> Error l
            | Ok _, Error e -> Error [e]
            | Error l, Error h -> Error (h::l)
        ) (Ok []) list

[<AutoOpen>]
module Auto =
    let inline mapFst f (a,b) = f a,b
    let inline fst3 (i,_,_) = i
    let inline snd3 (_,i,_) = i
    let inline trd (_,_,i) = i
    let (<*>) = Result.apply
    let inline zigzag (i:int) = (i <<< 1) ^^^ (i >>> 31) |> uint32
    let inline unzigzag (i:uint32) = int(i >>> 1) ^^^ -int(i &&& 1u)
    let inline zigzag64 (i:int64) = (i <<< 1) ^^^ (i >>> 63) |> uint64
    let inline unzigzag64 (i:uint64) = int64(i >>> 1) ^^^ -int64(i &&& 1UL)
    let private someunit = Some()
    let (|IsText|_|) str (Text s) =
        if String.Equals(str, s, StringComparison.OrdinalIgnoreCase) then
            someunit
        else None
    let tryCast (o:obj) : 'a option = // TODO: maybe should be Option.tryCast
        match o with
        | :? 'a as a -> Some a
        | _ -> None
    let memoize (f:'a->'b) =
        let d = ResizeMap HashIdentity.Structural
        fun a ->
            let mutable b = Unchecked.defaultof<_>
            if d.TryGetValue(a,&b) then b
            else
                b <- f a
                d.Add(a,b)
                b

[<Struct>]
type Date = // TODO: pick a better start date
    | Date of uint32
    static member (-) (Date a, Date b) = int(a - b)
    static member (+) (Date a, b: int) = Date(uint32(int a + b))
    static member (-) (Date a, b: int) = Date(uint32(int a - b))

module Date =
    let minValue = Date 0u
    let maxValue = Date UInt32.MaxValue
    let ofDateTime (d:DateTime) =
        d.Ticks / TimeSpan.TicksPerDay |> uint32 |> Date
    let toDateTime (Date d) =
        int64 d * TimeSpan.TicksPerDay |> DateTime

[<Struct>]
type Time =
    | Time of int64

module Time =
    let toInt64 (Time ticks) =
        ticks
    let toDate (Time ticks) =
        ticks / TimeSpan.TicksPerDay |> uint32 |> Date
    let ofDateTime (d:DateTime) =
        Time d.Ticks
    let toDateTime (Time t) =
        DateTime t

[<Struct>]
type Tx = Tx of uint32
    
module Tx =
    let maxValue = Tx UInt32.MaxValue

[<Struct>]
type EntityType =
    | EntityType of uint32
    static member entityType = EntityType 0u
    static member attribute = EntityType 1u
    static member transaction = EntityType 2u

[<Struct>]
type Entity =
    | Entity of EntityType * uint32

[<Struct>]
type AttributeId =
    | AttributeId of uint32
    static member uri = AttributeId 0u
    static member time = AttributeId 1u
    static member attribute_type = AttributeId 2u
    static member attribute_isset = AttributeId 3u

[<Struct>]
type TextId =
    internal
    | TextId of uint32

[<Struct>]
type DataId =
    internal
    | DataId of uint32

[<Struct>]
type Uri =
    internal
    | Uri of uint32

type Datum = Entity * AttributeId * Date * int64

type TransactionData = {
    TransactionDatum: (AttributeId * int64) list
    EntityDatum: Datum list
    Creates: Entity[]
    Text: Text[]
    Data: byte[][]
}