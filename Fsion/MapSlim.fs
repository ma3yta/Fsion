namespace Fsion

open System

[<Struct>]
type private Entry<'TKey,'TValue> =
    val mutable bucket : int
    val mutable key : 'TKey
    val mutable value : 'TValue
    val mutable next : int

type private InitialHolder<'TKey,'TValue>() =
    static let initial = Array.zeroCreate<Entry<'TKey,'TValue>> 1
    static member inline Initial = initial

type MapSlim<'TKey,'TValue when 'TKey : equality> =
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

    member m.TryGet(key:'TKey) : 'TValue option =
        let entries = m.entries
        let rec loop i =
            if uint32 i >= uint32 entries.Length then None
            elif key = entries.[i].key then Some entries.[i].value
            else loop entries.[i].next
        loop (entries.[key.GetHashCode() &&& (entries.Length - 1)].bucket - 1)

    member private m.Resize() =
        let mutable count = m.count
        let entries = Array.zeroCreate<Entry<_,_>> (m.entries.Length*2)
        Array.Copy(m.entries, 0, entries, 0, count)
        while count > 0 do
            count <- count - 1
            let bi = entries.[count].key.GetHashCode() &&& (entries.Length - 1)
            entries.[count].next <- entries.[bi].bucket - 1
            entries.[bi].bucket <- count + 1
        m.entries <- entries

    member private m.AddKey(key:'TKey, bucketIndex:int) =
        let mutable bucketIndex = bucketIndex
        let mutable i = m.count
        if i = 0 || i = m.entries.Length then
            m.Resize()
            bucketIndex <- key.GetHashCode() &&& (m.entries.Length - 1)
        let entries = m.entries
        entries.[i].key <- key
        entries.[i].next <- entries.[bucketIndex].bucket-1
        entries.[bucketIndex].bucket <- i+1
        m.count <- m.count + 1
        &entries.[i].value

    member m.RefGet(key:'TKey) : 'TValue byref =
        let entries = m.entries
        let mutable bucketIndex = key.GetHashCode() &&& (entries.Length - 1)
        let mutable i = entries.[bucketIndex].bucket - 1
        while uint32 i < uint32 entries.Length && key <> entries.[i].key do
            i <- entries.[i].next
        if i = -1 then &m.AddKey(key, bucketIndex)
        else &entries.[i].value