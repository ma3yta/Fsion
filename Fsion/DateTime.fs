namespace Fsion

open System
open System.Diagnostics

[<Struct>]
type Date = // TODO: pick a better start date
    | Date of uint32
    static member (-) (Date a, Date b) = int(a - b)
    static member (+) (Date a, b: int) = Date(uint32(int a + b))
    static member (-) (Date a, b: int) = Date(uint32(int a - b))

module Date =
    let minValue = Date 0u
    let maxValue = Date UInt32.MaxValue
    let ofDateTime (d:DateTime) =
        d.Ticks / TimeSpan.TicksPerDay |> uint32 |> Date
    let toDateTime (Date d) =
        int64 d * TimeSpan.TicksPerDay |> DateTime

[<Struct>]
type Time =
    | Time of int64

module NTP =
    open System.Net.Sockets
    
    let query (server:string) =
        let computeTime (data:byte[]) offset =
            let intPart = (int64 data.[offset+0] <<< 24) ||| (int64 data.[offset+1] <<< 16)
                      ||| (int64 data.[offset+2] <<< 08) ||| (int64 data.[offset+3])
            let fraPart = (int64 data.[offset+4] <<< 24) ||| (int64 data.[offset+5] <<< 16)
                      ||| (int64 data.[offset+6] <<< 08) ||| (int64 data.[offset+7])
            intPart * TimeSpan.TicksPerSecond
            + fraPart * TimeSpan.TicksPerSecond / 0x100000000L
            + 599266080000000000L // DateTime(1900, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).Ticks
        async {
            try
                let data = Array.zeroCreate 48
                Array.set data 0 27uy
                use socket = new Socket(AddressFamily.InterNetwork,SocketType.Dgram,ProtocolType.Udp,ReceiveTimeout=3000)
                do! Async.FromBeginEnd(server, (fun (s,c,o) -> socket.BeginConnect(s,123,c,o)), socket.EndConnect)
                let t1 = Stopwatch.GetTimestamp()
                socket.Send data |> ignore
                socket.Receive data |> ignore
                let t4 = Stopwatch.GetTimestamp()
                let t2 = computeTime data 32
                let t3 = computeTime data 40
                return Some(t1+(t4-t1)/2L,Time(t2+(t3-t2)/2L))
            with | _ -> return None
        }
    let internal combine measurments =
        match List.sort measurments with
        | [] -> None
        | [x] -> Some x
        | (ts0,Time time0)::xs ->
            let a = xs |> List.sumBy (fun (ts,Time time) -> time-time0-(ts-ts0)*TimeSpan.TicksPerSecond/Stopwatch.Frequency)
            Some(ts0,time0+a/int64(List.length xs+1) |> Time)
    let average (servers:string list) =
        async {
            let! l = List.map query servers |> Async.sequential
            return List.choose id l |> combine
        }

module Time =
    let inline ticks (Time t) = t
    let ofDateTime (t:DateTime) =
        assert(t.Kind=DateTimeKind.Utc)
        Time t.Ticks
    let toDateTime (Time t) = DateTime t
    let mutable private timestampAndTime =
        let ts = Stopwatch.GetTimestamp()
        let n = DateTime.UtcNow
        struct (ts, n.Ticks)
    let synchronise tsAndt = timestampAndTime<-tsAndt
    let now() =
        let struct (timestamp0,time0) = timestampAndTime
        time0+((Stopwatch.GetTimestamp()-timestamp0)*TimeSpan.TicksPerSecond)/Stopwatch.Frequency