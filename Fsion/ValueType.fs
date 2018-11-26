namespace Fsion

type FsionType =
    | TypeType
    | TypeBool
    | TypeInt
    | TypeInt64
    | TypeUri
    | TypeDate
    | TypeTime
    | TypeTextId
    | TypeDataId
    member i.Encode() =
        match i with
        | TypeType -> 1L
        | TypeBool -> 2L
        | TypeInt -> 3L
        | TypeInt64 -> 4L
        | TypeUri -> 5L
        | TypeDate -> 6L
        | TypeTime -> 7L
        | TypeTextId -> 8L
        | TypeDataId -> 9L
    static member Decode i =
        match i with
        | 1L -> TypeType
        | 2L -> TypeBool
        | 3L -> TypeInt
        | 4L -> TypeInt64
        | 5L -> TypeUri
        | 6L -> TypeDate
        | 7L -> TypeTime
        | 8L -> TypeTextId
        | 9L -> TypeDataId
        | _ -> TypeInt
    member i.Name =
        match i with
        | TypeType -> Text "type"
        | TypeBool -> Text "bool"
        | TypeInt -> Text "int"
        | TypeInt64 -> Text "int64"
        | TypeUri -> Text "uri"
        | TypeDate -> Text "date"
        | TypeTime -> Text "time"
        | TypeTextId -> Text "textid"
        | TypeDataId -> Text "dataid"
    static member Parse text : Result<FsionType,Text> =
        match text with
        | IsText "type" -> TypeType |> Ok
        | IsText "bool" -> TypeBool |> Ok
        | IsText "int" -> TypeInt |> Ok
        | IsText "int64" -> TypeInt64 |> Ok
        | IsText "uri" -> TypeUri |> Ok
        | IsText "date" -> TypeDate |> Ok
        | IsText "time" -> TypeTime |> Ok
        | IsText "textid" -> TypeTextId |> Ok
        | IsText "dataid" -> TypeDataId |> Ok
        | Text s -> "unknown type: " + s |> Text |> Error

type FsionValue =
    | FsionType of FsionType
    | FsionBool of bool
    | FsionInt of int
    | FsionInt64 of int64
    | FsionUri of Fsion.Uri
    | FsionDate of Date
    | FsionTime of Time
    | FsionTextId of TextId
    | FsionDataId of DataId

module FsionValue =

    let encodeType (i:FsionType option) =
        match i with
        | None -> 0L
        | Some i -> i.Encode()

    let decodeType i =
        match i with
        | 0L -> None
        | i -> FsionType.Decode i |> Some

    let encodeBool i =
        match i with
        | None -> 0L
        | Some i -> if i then 1L else -1L
    
    let decodeBool i =
        match i with
        | 0L -> None
        | 1L -> Some true
        | -1L -> Some false
        | i -> failwithf "bool ofint %i" i

    let encodeInt i =
        match i with
        | None -> 0L
        | Some i -> if i>0 then int64 i else int64(i-1)

    let decodeInt i =
        match i with
        | 0L -> None
        | i when i>0L -> int32 i |> Some
        | i -> int32 i + 1 |> Some

    let encodeInt64 i =
        match i with
        | None -> 0L
        | Some i -> if i>0L then i else i-1L

    let decodeInt64 i =
        match i with
        | 0L -> None
        | i when i>0L -> Some i
        | i -> i + 1L |> Some

    let encodeUri i =
        match i with
        | None -> 0L
        | Some(Uri i) -> unzigzag(i+1u) |> int64
    
    let decodeUri i =
        match i with
        | 0L -> None
        | i -> Fsion.Uri(zigzag(int32 i) - 1u) |> Some

    let encodeDate i =
        match i with
        | None -> 0L
        | Some(Date i) -> unzigzag(i+1u) |> int64

    let decodeDate i =
        match i with
        | 0L -> None
        | i -> Date(zigzag(int32 i) - 1u) |> Some

    let encodeTime i =
        match i with
        | None -> 0L
        | Some(Time i) -> if i>0L then i else i-1L

    let decodeTime i =
        match i with
        | 0L -> None
        | i when i>0L -> Time i |> Some
        | i -> Time(i+1L) |> Some

    let encodeTextId i =
        match i with
        | None -> 0L
        | Some(TextId i) -> unzigzag(i+1u) |> int64

    let decodeTextId i =
        match i with
        | 0L -> None
        | i -> TextId(zigzag(int32 i) - 1u) |> Some

    let encodeDataId i =
        match i with
        | None -> 0L
        | Some(DataId i) -> unzigzag(i+1u) |> int64

    let decodeDataId i =
        match i with
        | 0L -> None
        | i -> DataId(zigzag(int32 i) - 1u) |> Some