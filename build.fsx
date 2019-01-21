#r "paket:
nuget Fake.Core.Xml
nuget Fake.DotNet.Cli
nuget Fake.DotNet.Paket
nuget Fake.Tools.Git
nuget Fake.Api.GitHub
nuget Fake.Core.Target
nuget Fake.Core.Environment
nuget Fake.Core.UserInput
nuget Fake.DotNet.AssemblyInfoFile
nuget Fake.BuildServer.AppVeyor
nuget Fake.BuildServer.Travis
nuget Fake.Core.ReleaseNotes //"
#load "./.fake/build.fsx/intellisense.fsx"
#if !FAKE
#r "netstandard"
#endif

open System
open Fake.Core
open Fake.DotNet
open Fake.Tools
open Fake.Api
open Fake.IO
open Fake.Core.TargetOperators
open Fake.IO.Globbing.Operators
open Fake.BuildServer

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let configuration = Environment.environVarOrDefault "Configuration" "Release"
let release = ReleaseNotes.load "RELEASE_NOTES.md"
let description = "EAVT (Entity Attribute Value Time) database for F#"
let tags = "fsharp database time-series"
let authors = "Anthony Lloyd"
let owners = "Anthony Lloyd"
let projectUrl = "https://github.com/AnthonyLloyd/Fsion"
let iconUrl = "https://raw.githubusercontent.com/AnthonyLloyd/Fsion/master/docs/fsion.png"
let licenceUrl = "https://github.com/AnthonyLloyd/Fsion/blob/master/LICENSE"
let copyright = "Copyright 2018"
let mutable dotnetExePath = "dotnet"

BuildServer.install [
    Travis.Installer
    AppVeyor.Installer
]

Target.create "Clean" (fun _ ->
    !!"./**/bin/" ++ "./**/obj/" |> Shell.cleanDirs
)
let normaliseFileToLFEnding filename =
    let s = File.readAsString filename
    s.Replace(String.WindowsLineBreaks,String.LinuxLineBreaks)
    |> File.writeString false filename

Target.create "AssemblyInfo" (fun _ ->
    let createAssemblyInfo project =
        let filename = project+"/AssemblyInfo.fs"
        AssemblyInfoFile.createFSharpWithConfig filename [
            AssemblyInfo.Title project
            AssemblyInfo.Product project
            AssemblyInfo.Copyright copyright
            AssemblyInfo.Description description
            AssemblyInfo.Version release.AssemblyVersion
            AssemblyInfo.FileVersion release.AssemblyVersion
            AssemblyInfo.InternalsVisibleTo "Fsion.Tests"
        ] (AssemblyInfoFileConfig(false,false,"Fsion"))
        normaliseFileToLFEnding filename
    createAssemblyInfo "Fsion"
    createAssemblyInfo "Fsion.API"
)

Target.create "ProjectVersion" (fun _ ->
    let setProjectVersion project =
        let filename = project+"/"+project+".fsproj"
        Xml.pokeInnerText filename
            "Project/PropertyGroup/Version" release.NugetVersion
        normaliseFileToLFEnding filename
    setProjectVersion "Fsion"
)
let build project =
    DotNet.build (fun p ->
    { p with
        Configuration = DotNet.BuildConfiguration.Custom configuration
        Common = DotNet.Options.withDotNetCliPath dotnetExePath p.Common
                 |> DotNet.Options.withCustomParams (Some "--no-dependencies")
    }) project

Target.create "Build" (fun _ ->
    build "Fsion.API/Fsion.API.fsproj"
    build "Fsion/Fsion.fsproj"
)
let isOk (pr:ProcessResult) =
    if not pr.OK then failwithf "%A" pr

Target.create "RunTest" (fun _ ->

    let runTest project =
        DotNet.exec (DotNet.Options.withDotNetCliPath dotnetExePath)
             "run" ("-f netcoreapp2.2 -c release -p " + project)
        |> isOk

        project + ".TestResults.xml"
        |> Path.combine (Path.combine __SOURCE_DIRECTORY__ "bin")
        |> Trace.publish (ImportData.Nunit NunitDataVersion.Nunit)

    runTest "Fsion.Tests"
)

Target.create "Pack" (fun _ ->
    let pack project =
        let packParameters =
            [
                "--no-build"
                "--no-restore"
                sprintf "/p:Title=\"%s\"" project
                "/p:PackageVersion=" + release.NugetVersion
                sprintf "/p:Authors=\"%s\"" authors
                sprintf "/p:Owners=\"%s\"" owners
                "/p:PackageRequireLicenseAcceptance=false"
                sprintf "/p:Description=\"%s\"" description
                sprintf "/p:PackageReleaseNotes=\"%O\""
                    ((String.toLines release.Notes).Replace(",",""))
                sprintf "/p:Copyright=\"%s\"" copyright
                sprintf "/p:PackageTags=\"%s\"" tags
                sprintf "/p:PackageProjectUrl=\"%s\"" projectUrl
                sprintf "/p:PackageIconUrl=\"%s\"" iconUrl
                sprintf "/p:PackageLicense=\"%s\"" licenceUrl
            ] |> String.concat " "
        "pack "+project+"/"+project+".fsproj -c "+configuration + " -o ../bin "
        + packParameters
        |> DotNet.exec id <| ""
        |> ignore
    pack "Fsion"
    pack "Fsion.API"
)

Target.create "Push" (fun _ ->
    Paket.push (fun p -> { p with WorkingDir = "bin" })
)

Target.create "Release" (fun _ ->
    let gitOwner = "AnthonyLloyd"
    let gitName = "Fsion"
    let gitOwnerName = gitOwner + "/" + gitName
    let remote =
        Git.CommandHelper.getGitResult "" "remote -v"
        |> Seq.tryFind (fun s -> s.EndsWith "(push)" && s.Contains gitOwnerName)
        |> function | None -> "ssh://github.com/" + gitOwnerName
                    | Some s -> s.Split().[0]

    Git.Staging.stageAll ""
    Git.Commit.exec "" (sprintf "Bump version to %s" release.NugetVersion)
    Git.Branches.pushBranch "" remote (Git.Information.getBranchName "")

    Git.Branches.tag "" release.NugetVersion
    Git.Branches.pushTag "" remote release.NugetVersion

    Environment.environVar "GITHUB_TOKEN"
    |> GitHub.createClientWithToken
    |> GitHub.draftNewRelease gitOwner gitName release.NugetVersion
                              release.SemVer.PreRelease.IsSome release.Notes
    |> GitHub.publishDraft
    |> Async.RunSynchronously
)

Target.create "All" ignore

"Clean"
==> "AssemblyInfo"
==> "ProjectVersion"
==> "Build"
==> "RunTest"
==> "Pack"
==> "All"
==> "Push"
==> "Release"

Target.runOrDefaultWithArguments "All"
