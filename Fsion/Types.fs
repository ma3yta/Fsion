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
    let next (Tx i) = Tx (i+1u)

[<Struct>]
type EntityType =
    | EntityType of uint32

module EntityType =
    module Int =
        [<Literal>]
        let transaction = 0u
        [<Literal>]
        let entityType = 1u
        [<Literal>]
        let attribute = 1u
    let transaction = EntityType Int.transaction
    let entityType = EntityType Int.entityType
    let attribute = EntityType Int.attribute

[<Struct>]
type Entity =
    | Entity of EntityType * uint32

[<Struct>]
type AttributeId =
    | AttributeId of uint32

module AttributeId =
    module Int =
        [<Literal>]
        let uri = 0u
        [<Literal>]
        let name = 1u
        [<Literal>]
        let time = 2u
        [<Literal>]
        let attribute_type = 3u
        [<Literal>]
        let attribute_isset = 4u
        [<Literal>]
        let transaction_based_on = 5u
    let uri = AttributeId Int.uri
    let name = AttributeId Int.name
    let time = AttributeId Int.time
    let attribute_type = AttributeId Int.attribute_type
    let attribute_isset = AttributeId Int.attribute_isset
    let transaction_based_on = AttributeId Int.transaction_based_on


[<Struct;CustomEquality;CustomComparison>]
type EntityAttribute =
    | EntityAttribute of Entity * AttributeId
    interface IEquatable<EntityAttribute> with
        member m.Equals (EntityAttribute(oe,oa)) =
            let (EntityAttribute(e,a)) = m
            oe = e && oa = a
    override m.Equals(o:obj) =
        match o with
        | :? EntityAttribute as ea ->
            let (EntityAttribute(oe,oa)) = m
            let (EntityAttribute(e,a)) = m
            oe = e && oa = a
        | _ -> false
    interface IComparable with
        member m.CompareTo o =
            match o with
            | :? EntityAttribute as ea ->
                let (EntityAttribute(oe,oa)) = m
                let (EntityAttribute(e,a)) = m
                0
            | _ -> 0
    override m.GetHashCode() =
        let (EntityAttribute(e,a)) = m
        e.GetHashCode() ^^^ a.GetHashCode()

[<Struct>]
type TextId =
    internal
    | TextId of uint32

[<Struct>]
type Data =
    internal
    | Data of byte[]

[<Struct>]
type DataId =
    internal
    | DataId of uint32

[<Struct>]
type Uri =
    internal
    | Uri of uint32

type Datum = Entity * AttributeId * Date * int64

type TxData = {
    Text: Text list // set? Needs client to make unique for effiecient serialization
    Data: Data list
    Datum: Datum list1
}