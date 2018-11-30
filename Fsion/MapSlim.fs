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
        let oldEntries = m.entries
        let entries = Array.zeroCreate<Entry<_,_>> (oldEntries.Length*2)
        for i = 0 to oldEntries.Length-1 do
            entries.[i].value <- oldEntries.[i].value
            entries.[i].key <- oldEntries.[i].key
            let bi = entries.[i].key.GetHashCode() &&& (entries.Length-1)
            entries.[i].next <- entries.[bi].bucket-1
            entries.[bi].bucket <- i+1
        m.entries <- entries

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member private m.AddKey(key:'TKey, hashCode:int) =
        let i = m.count
        if i = 0 && m.entries.Length = 1 then
            m.entries <- Array.zeroCreate 2
        elif i = m.entries.Length then m.Resize()
        let entries = m.entries
        entries.[i].key <- key
        let bucketIndex = hashCode &&& (entries.Length-1)
        entries.[i].next <- entries.[bucketIndex].bucket-1
        entries.[bucketIndex].bucket <- i+1
        m.count <- i+1
        &entries.[i].value

    member m.Set(key:'TKey, value: 'TValue) =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[hashCode &&& (entries.Length-1)].bucket-1
        while uint32 i < uint32 entries.Length && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = -1 then
            let v = &m.AddKey(key, hashCode)
            v <- value
        else entries.[i].value <- value

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

    member m.Item(i) : 'TKey * 'TValue =
        let entries = m.entries.[i]
        entries.key, entries.value

[<AutoOpen>]
module MapSlimAutoOpen =
    let memoize (f:'a->'b) =
        let d = MapSlim()
        fun a ->
            let mutable isNew = false
            let b = &d.RefGet(a, &isNew)
            if isNew then b <- f a
            b

[<Struct>]
type private Entry2<'TKey,'TValue> =
    val mutable bucket : int
    val mutable next : int
    val mutable key : 'TKey
    val mutable value : 'TValue

type private InitialHolder2<'TKey,'TValue>() =
    static let initial = Array.zeroCreate<Entry2<'TKey,'TValue>> 1
    static member inline Initial = initial

type MapSlim2<'TKey,'TValue when 'TKey : equality and 'TKey :> IEquatable<'TKey>> =
    val mutable private count : int
    val mutable private entries : Entry2<'TKey,'TValue>[]
    new() = {count=0; entries=InitialHolder2.Initial}
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
        let oldEntries = m.entries
        let entries = Array.zeroCreate<Entry2<_,_>> (oldEntries.Length*2)
        for i = 0 to oldEntries.Length-1 do
            entries.[i].value <- oldEntries.[i].value
            entries.[i].key <- oldEntries.[i].key
            let bi = entries.[i].key.GetHashCode() &&& (entries.Length-1)
            entries.[i].next <- entries.[bi].bucket-1
            entries.[bi].bucket <- i+1
        m.entries <- entries

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member private m.AddKey(key:'TKey, hashCode:int) =
        let i = m.count
        if i = 0 && m.entries.Length = 1 then
            m.entries <- Array.zeroCreate 2
        elif i = m.entries.Length then m.Resize()
        let entries = m.entries
        entries.[i].key <- key
        let bucketIndex = hashCode &&& (entries.Length-1)
        entries.[i].next <- entries.[bucketIndex].bucket-1
        entries.[bucketIndex].bucket <- i+1
        m.count <- i+1
        &entries.[i].value

    member m.Set(key:'TKey, value: 'TValue) =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[hashCode &&& (entries.Length-1)].bucket-1
        while uint32 i < uint32 entries.Length && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = -1 then
            let v = &m.AddKey(key, hashCode)
            v <- value
        else entries.[i].value <- value

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

    member m.Item(i) : 'TKey * 'TValue =
        let entries = m.entries.[i]
        entries.key, entries.value