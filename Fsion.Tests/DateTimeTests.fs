module DateTimeTests

open Expecto
open Fsion

let dateTimeTests =

    testList "date time" [

        testAsync "ntp query" {
            let data = NTP.query "time.windows.com" |> Async.RunSynchronously
            Expect.isSome data "some"
            //let t = Option.get data |> snd |> Time.toDateTime
            //printfn "time: %A" t
        }

        testAsync "ntp query many" {
            let ntpServers = [
                "time.windows.com"
                "time.apple.com"
                "time.nist.gov"
                "pool.ntp.org"
                "extntp1.inf.ed.ac.uk"
                "ntp.exnet.com"
            ]
            List.map (NTP.query >> Async.RunSynchronously) ntpServers
            |> printfn "All: %A"
            let data =
                NTP.average ntpServers |> Async.RunSynchronously
            Expect.isSome data "some"
        }
    ]