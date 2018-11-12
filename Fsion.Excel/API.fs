namespace Fsion.Excel

open ExcelDna.Integration

type private FsionFunction() =
    inherit ExcelFunctionAttribute(
        Category = "Fsion",
        IsExceptionSafe = true,
        IsThreadSafe = true
    )

module API =

    [<FsionFunction(
        Description = "my test method",
        HelpTopic = "http://www.ibm.com"
    )>]
    let Test ([<ExcelArgument(Description="is an important parameter.")>]i:int)
             (j:int)
             =
        Array2D.init i j (fun i j ->
            sprintf "%c%i" (char(65+j)) (i+1) :> obj
        )

    [<FsionFunction(
        Description = "fsion query",
        HelpTopic = "http://news.bbc.co.uk"
    )>]
    let FQuery ([<ExcelArgument(Description="the fsion query")>] query:string) =
        Array2D.init 1 1 (fun i j -> query :> obj)

    [<FsionFunction(
        Description = "fsion command",
        HelpTopic = "sss"
    )>]
    let FCommand ([<ExcelArgument(Description="the fsion command set")>] table:obj[,]) =
        table.[0,0]