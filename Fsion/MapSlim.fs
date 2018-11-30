namespace Fsion

open System
open System.Runtime.CompilerServices

[<Struct>]
type private Entry<'TKey,'TValue> =
    val mutable bucket : int
    val mutable key : 'TKey
    val mutable value : 'TValue
    val mutable next : int

type private InitialHolder<'TKey,'TValue>() =
    static let initial = Array.zeroCreate<Entry<'TKey,'TValue>> 1
    static member inline Initial = initial

type MapSlim<'TKey,'TValue when 'TKey : equality and 'TKey :> IEquatable<'TKey>> =
    val mutable private count : int
    val mutable private entries : Entry<'TKey,'TValue>[]
    new() = {count=0; entries=InitialHolder.Initial}
    new(capacity:int) = {
        count = 0
        entries =
            let inline powerOf2 v =
                if v &&& (v-1) = 0 then v
                else
                    let rec twos i =
                        if i>=v then i
                        else twos (i*2)
                    twos 2
            powerOf2 capacity |> Array.zeroCreate
    }

    member m.Count = m.count

    member private m.Resize() =
        let entries = Array.zeroCreate<Entry<_,_>> (m.entries.Length*2)
        for i = 0 to m.count-1 do
            entries.[i].value <- m.entries.[i].value
            entries.[i].key <- m.entries.[i].key
            let bi = entries.[i].key.GetHashCode() &&& (entries.Length-1)
            entries.[i].next <- entries.[bi].bucket-1
            entries.[bi].bucket <- i+1
        m.entries <- entries

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member private m.AddKey(key:'TKey, hashCode:int) =
        let i = m.count
        if i = m.entries.Length || m.entries.Length = 1 then
            m.Resize()
        let entries = m.entries
        entries.[i].key <- key
        let bucketIndex = hashCode &&& (entries.Length-1)
        entries.[i].next <- entries.[bucketIndex].bucket-1
        entries.[bucketIndex].bucket <- i+1
        m.count <- i+1
        &entries.[i].value

    member m.RefGet(key:'TKey) : 'TValue byref =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[hashCode &&& (entries.Length-1)].bucket-1
        while uint32 i < uint32 entries.Length && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = -1 then &m.AddKey(key, hashCode)
        else &entries.[i].value

    member m.RefGet(key:'TKey, added: bool outref) : 'TValue byref =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[hashCode &&& (entries.Length-1)].bucket-1
        while uint32 i < uint32 entries.Length && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = -1 then
            added <- true
            &m.AddKey(key, hashCode)
        else
            added <- false
            &entries.[i].value

    member m.TryGet(key:'TKey) : 'TValue option =
        let entries = m.entries
        let mutable i = entries.[key.GetHashCode() &&& (entries.Length-1)].bucket-1
        while uint32 i < uint32 entries.Length && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = -1 then None
        else Some entries.[i].value