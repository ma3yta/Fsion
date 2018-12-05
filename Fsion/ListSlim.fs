namespace Fsion

open System

type ListSlim<'k> =
    val mutable private count : int
    val mutable private entries : 'k[]
    new() = {count=0; entries=null}
    new(capacity:int) = {count = 0; entries = Array.zeroCreate capacity}

    member m.Count = m.count

    member private m.Resize() =
        let oldEntries = m.entries
        let entries = Array.zeroCreate (oldEntries.Length*2)
        Array.Copy(oldEntries, 0, entries, 0, oldEntries.Length)
        m.entries <- entries

    member m.Add(key:'k) =
        let i = m.count
        if i = 0 && isNull m.entries then
            m.entries <- Array.zeroCreate 1
        elif i = m.entries.Length then m.Resize()
        let entries = m.entries
        entries.[i] <- key
        m.count <- i+1
        i

    member m.Item(i) = m.entries.[i]