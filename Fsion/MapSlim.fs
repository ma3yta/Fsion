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
    static let initial = Array.zeroCreate<Entry<'TKey,'TValue>> 2
    static member inline Initial = initial

type MapSlim<'TKey,'TValue when 'TKey : equality and 'TKey :> IEquatable<'TKey>> =
    val mutable private countWithDefault : int
    val mutable private entries : Entry<'TKey,'TValue>[]
    new() = {countWithDefault=1; entries=InitialHolder.Initial}
    new(capacity:int) = {
        countWithDefault = 1
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

    member m.Count = m.countWithDefault-1

    member private m.Resize() =
        let oldEntries = m.entries
        let entries = Array.zeroCreate<Entry<_,_>> (oldEntries.Length*2-1)
        if oldEntries.Length <> 2 then
            for i = oldEntries.Length-1 downto 1 do
                entries.[i].value <- oldEntries.[i].value
                entries.[i].key <- oldEntries.[i].key
                let bi = (entries.[i].key.GetHashCode() &&& (entries.Length-2)) + 1
                entries.[i].next <- entries.[bi].bucket
                entries.[bi].bucket <- i
        m.entries <- entries

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member private m.AddKey(key:'TKey, hashCode:int) =
        let i = m.countWithDefault
        if i = m.entries.Length || (i = 1 && m.entries.Length = 2) then m.Resize()
        let entries = m.entries
        entries.[i].key <- key
        let bucketIndex = (hashCode &&& (entries.Length-2)) + 1
        entries.[i].next <- entries.[bucketIndex].bucket
        entries.[bucketIndex].bucket <- i
        m.countWithDefault <- i+1
        &entries.[i].value

    member m.Set(key:'TKey, value: 'TValue) =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[(hashCode &&& (entries.Length-2)) + 1].bucket
        while i <> 0 && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = 0 then
            let v = &m.AddKey(key, hashCode)
            v <- value
        else entries.[i].value <- value

    member m.GetRef(key:'TKey) : 'TValue byref =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[(hashCode &&& (entries.Length-2)) + 1].bucket
        while i <> 0 && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = 0 then &m.AddKey(key, hashCode)
        else &entries.[i].value

    member m.GetRef(key:'TKey, added: bool outref) : 'TValue byref =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[(hashCode &&& (entries.Length-2)) + 1].bucket
        while i <> 0 && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = 0 then
            added <- true
            &m.AddKey(key, hashCode)
        else
            added <- false
            &entries.[i].value

    member m.GetOption(key:'TKey) : 'TValue voption =
        let entries = m.entries
        let mutable i = entries.[(key.GetHashCode() &&& (entries.Length-2)) + 1].bucket
        while i <> 0 && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i = 0 then ValueNone
        else ValueSome entries.[i].value

    member m.Item(i) : 'TKey * 'TValue =
        let entries = m.entries.[i+1]
        entries.key, entries.value

[<AutoOpen>]
module MapSlimAutoOpen =
    let memoize (f:'a->'b) =
        let d = MapSlim()
        fun a ->
            let mutable isNew = false
            let b = &d.GetRef(a, &isNew)
            if isNew then b <- f a
            b