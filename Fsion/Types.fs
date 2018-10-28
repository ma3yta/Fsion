namespace Fsion

open System
open System.Diagnostics

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
    | EntityType of byte
    static member tx = EntityType 0uy
    static member entityType = EntityType 1uy
    static member attribute = EntityType 2uy

[<Struct>]
type Entity =
    | Entity of EntityType * uint32

[<CustomEquality;CustomComparison;DebuggerDisplay("{Uri}")>]
type Attribute = {
    Id: uint32
    Uri: string
    IsSet: bool
    IsString: bool
    Doc: string
} with
    static member uri = {
        Id = 0u
        Uri = "uri"
        IsSet = true
        IsString = true
        Doc = "Unique reference for an entity. Can be updated but the previous uris will continue to be other unique references to the entity."
    }
    static member time = {
        Id = 1u
        Uri = "time"
        IsSet = false
        IsString = false
        Doc = "Time the transaction was committed to the database."
    }
    member x.Entity =
        Entity(EntityType.attribute, x.Id)
    override x.GetHashCode() =
        int x.Id
    override x.Equals o =
        x.Id = (o :?> Attribute).Id
    interface IEquatable<Attribute> with
        member x.Equals(o:Attribute) =
            x.Id = o.Id
    interface IComparable with
        member x.CompareTo o =
            compare x.Id ((o :?> Attribute).Id)

type TextId =
    internal
    | TextId of int

type DataId =
    internal
    | DataId of int