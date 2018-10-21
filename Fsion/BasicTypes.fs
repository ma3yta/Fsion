namespace Fsion

open System

[<Struct>]
type Date =
    | Date of uint32
    static member (-) (Date a, Date b) = int(a - b)
    static member (+) (Date a, b: int) = Date(uint32(int a + b))
    static member (-) (Date a, b: int) = Date(uint32(int a - b))

module Date =
    let minValue = Date 0u
    let fromDateTime (d:DateTime) = d.Ticks / TimeSpan.TicksPerDay |> uint32 |> Date

[<Struct>]
type Tx = Tx of uint32

module Tx =
    let maxValue = Tx UInt32.MaxValue

type Datum = Date * Tx * int64