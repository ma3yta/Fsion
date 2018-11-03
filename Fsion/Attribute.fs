namespace Fsion

type ValueType<'a> = private {
    ToInt: 'a -> int64
    OfInt: int64 -> 'a
}

module ValueType =

    let Int = {
        ToInt = fun (i:int32) -> int64 i
        OfInt = fun (i:int64) -> int32 i
    }

    let Uri = {
        ToInt = fun (Uri i) -> unzigzag i |> int64
        OfInt = fun (i:int64) -> int32 i |> zigzag |> Uri
    }

    let Time = {
        ToInt = fun (Time t) -> t
        OfInt = fun (t:int64) -> Time t
    }

type Attribute<'a> = {
    Id: AttributeId
    ValueType: ValueType<'a>
}

module Attribute =
    let fromId<'a> (db:Database) (AttributeId aid) : Attribute<'a> =
        failwith "hi"
    let fromUri<'a> (db:Database) (Uri uri) : Attribute<'a> =
        failwith "hi"

    let uri : Attribute<Uri> = { Id = AttributeId 0u; ValueType = ValueType.Uri }
    let time : Attribute<Time> = { Id = AttributeId 1u; ValueType = ValueType.Time }

    let read (db:Database) (e:Entity) (a:Attribute<'a>) (d:Date) (tx:Tx) =
        db.DataCache.Get (e,a.Id)
        |> Option.map (DataSeries.get d tx) // TODO: Need logic to check against date and tx for this type
        |> Option.map (trd >> a.ValueType.OfInt)