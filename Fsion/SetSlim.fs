namespace Fsion

open System
open System.Runtime.CompilerServices

[<Struct>]
type private Entry<'k> =
    val mutable bucket : int
    val mutable key : 'k
    val mutable next : int

type private InitialHolder<'k>() =
    static let initial = Array.zeroCreate<Entry<'k>> 1
    static member inline Initial = initial

type SetSlim<'k when 'k : equality and 'k :> IEquatable<'k>> =
    val mutable private count : int
    val mutable private entries : Entry<'k>[]
    new() = {count=0; entries=InitialHolder<_>.Initial}
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
        let entries = Array.zeroCreate<Entry<_>> (oldEntries.Length*2)
        for i = oldEntries.Length-1 downto 0 do
            entries.[i].key <- oldEntries.[i].key
            let bi = entries.[i].key.GetHashCode() &&& (entries.Length-1)
            entries.[i].next <- entries.[bi].bucket-1
            entries.[bi].bucket <- i+1
        m.entries <- entries

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member private m.AddKey(key:'k, hashCode:int) =
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
        i

    member m.Get(key:'k) =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[hashCode &&& (entries.Length-1)].bucket-1
        while i >= 0 && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i >= 0 then i
        else m.AddKey(key, hashCode)

    member m.GetOption(key:'k) =
        let entries = m.entries
        let hashCode = key.GetHashCode()
        let mutable i = entries.[hashCode &&& (entries.Length-1)].bucket-1
        while i >= 0 && not(key.Equals(entries.[i].key)) do
            i <- entries.[i].next
        if i >= 0 then ValueSome i
        else ValueNone

    member m.Item(i) : 'k =
        m.entries.[i].key