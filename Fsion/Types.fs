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
type AttributeId =
    | AttributeId of uint32
    static member uri = AttributeId 0u
    static member time = AttributeId 1u
    member a.Entity =
        let (AttributeId i) = a
        Entity(EntityType.attribute, i)

type TextId =
    internal
    | TextId of uint32

type DataId =
    internal
    | DataId of uint32

type Uri =
    internal
    | Uri of uint32

type TransactionData = {
    Text: Text[]
    Data: byte[][]
    Creates: Entity list
    EntityDatum: (Entity * AttributeId * Date * int64) list
    TransactionDatum: (AttributeId * int64) list
}