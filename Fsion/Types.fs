namespace Fsion

open System

[<Struct>]
type Text =
    private
    | Text of string

module Text =
    let ofString s =
        if String.IsNullOrWhiteSpace s then Text String.Empty
        else Text (s.Trim())
    let toString (Text s) = s

[<Struct>]
type Date =
    | Date of uint32
    static member (-) (Date a, Date b) = int(a - b)
    static member (+) (Date a, b: int) = Date(uint32(int a + b))
    static member (-) (Date a, b: int) = Date(uint32(int a - b))

module Date =
    let minValue = Date 0u
    let maxValue = Date UInt32.MaxValue
    let fromDateTime (d:DateTime) =
        d.Ticks / TimeSpan.TicksPerDay |> uint32 |> Date

[<Struct>]
type Time =
    | Time of int64

module Time =
    let toInt64 (Time ticks) =
        ticks
    let toDate (Time ticks) =
        ticks / TimeSpan.TicksPerDay |> uint32 |> Date

[<Struct>]
type Tx = Tx of uint32
    
module Tx =
    let maxValue = Tx UInt32.MaxValue
    
type Datum = Date * Tx * int64

[<Struct>]
type EntityType =
    | EntityType of uint32
    static member tx = EntityType 0u
    static member entityType = EntityType 1u
    static member attribute = EntityType 2u

[<Struct>]
type Entity =
    | Entity of EntityType * uint32

[<Struct>]
type Attribute =
    | Attribute of uint32
    static member uri = Attribute 0u
    static member time = Attribute 1u
    member a.Entity =
        let (Attribute i) = a
        Entity(EntityType.attribute, i)

type TextId =
    internal
    | TextId of int

type DataId =
    internal
    | DataId of int


type TransactionData = {
    Headers: (Attribute * int64) list
    Creates: (Entity * Attribute * Date * int64) list
    Updates: (Entity * Attribute * Date * int64) list
    Text: Text[]
    Data: byte[][]
}