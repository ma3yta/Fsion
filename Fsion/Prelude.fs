namespace Fsion

open System.Diagnostics.CodeAnalysis

[<AutoOpen>]
module Auto =
    let inline mapFst f (a,b) = f a,b
    let inline fst3 (i,_,_) = i
    let inline snd3 (_,i,_) = i
    let inline trd (_,_,i) = i

[<Struct;SuppressMessage("NameConventions","TypeNamesMustBePascalCase")>]
/// A non-empty list
type 'a list1 = private List1 of 'a list

module List1 =
    let head (List1 l) = List.head l
    let tail (List1 l) = List.tail l
    let init x xs = List1 (x::xs)
    let cons x (List1 xs) = List1 (x::xs)
    let tryOfList l = match l with | [] -> None | x::xs -> init x xs |> Some
    let toList (List1 l) = l
    let singleton s = List1 [s]
    let map mapper (List1 l) = List.map mapper l |> List1
    let sort (List1 l) = List.sort l |> List1
    let fold folder state (List1 list) = List.fold folder state list
    let tryPick chooser (List1 list) = List.tryPick chooser list
    let tryChoose chooser (List1 list) =
        match List.choose chooser list with | [] -> None | l -> List1 l |> Some
    let tryCollect mapping (List1 list) =
        List.choose mapping list |> List.collect toList |> tryOfList
    let append (List1 l1) (List1 l2) = List1(l1@l2)