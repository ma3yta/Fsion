namespace Fsion

type 'a ValueType = internal {
    ToInt: 'a option -> int64
    OfInt: int64 -> 'a option
}

module ValueType =

    let Bool = {
        ToInt =
            function
            | None -> 0L
            | Some i -> if i then 1L else -1L
        OfInt =
            function
            | 0L -> None
            | 1L -> Some true
            | -1L -> Some false
            | i -> failwithf "bool ofint %i" i
    }

    let Int = {
        ToInt =
            function
            | None -> 0L
            | Some i -> if i>0 then int64 i else int64(i-1)
        OfInt =
            function
            | 0L -> None
            | i when i>0L -> int32 i |> Some
            | i -> int32 i + 1 |> Some
    }

    let Int64 = {
        ToInt =
            function
            | None -> 0L
            | Some i -> if i>0L then i else i-1L
        OfInt =
            function
            | 0L -> None
            | i when i>0L -> Some i
            | i -> i + 1L |> Some
    }

    let Uri = {
        ToInt =
            function
            | None -> 0L
            | Some(Uri i) -> unzigzag(i+1u) |> int64
        OfInt =
            function
            | 0L -> None
            | i -> Uri(zigzag(int32 i) - 1u) |> Some
    }

    let Date = {
        ToInt =
            function
            | None -> 0L
            | Some(Date i) -> unzigzag(i+1u) |> int64
        OfInt =
            function
            | 0L -> None
            | i -> Date(zigzag(int32 i) - 1u) |> Some
    }

    let Time = {
        ToInt =
            function
            | None -> 0L
            | Some(Time i) -> if i>0L then i else i-1L
        OfInt =
            function
            | 0L -> None
            | i when i>0L -> Time i |> Some
            | i -> Time(i+1L) |> Some
    }

    let TextId = {
        ToInt =
            function
            | None -> 0L
            | Some(TextId i) -> unzigzag(i+1u) |> int64
        OfInt =
            function
            | 0L -> None
            | i -> TextId(zigzag(int32 i) - 1u) |> Some
    }

    let DataId = {
        ToInt =
            function
            | None -> 0L
            | Some(DataId i) -> unzigzag(i+1u) |> int64
        OfInt =
            function
            | 0L -> None
            | i -> DataId(zigzag(int32 i) - 1u) |> Some
    }

type Attribute<'a> = {
    Id: AttributeId
    ValueType: ValueType<'a>
}

// TODO: sets, typed entities, decimal

module Attribute =
    let fromId<'a> (db:Database) (AttributeId aid) : Attribute<'a> =
        failwith "hi"
    let fromUri<'a> (db:Database) (Uri uri) : Attribute<'a> =
        failwith "hi"

    let uri : Attribute<Uri> = { Id = AttributeId 0u; ValueType = ValueType.Uri }
    let time : Attribute<Time> = { Id = AttributeId 1u; ValueType = ValueType.Time }

    let get (db:Database) (e:Entity) (a:Attribute<'a>) (d:Date) (tx:Tx) =
        db.DataCache.Get (e,a.Id)
        |> Option.map (DataSeries.get d tx)
        |> Option.bind (fun (d,t,v) ->
            a.ValueType.OfInt v |> Option.map (fun i -> d,t,i))