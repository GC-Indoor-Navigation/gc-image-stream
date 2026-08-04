param(
    [string]$CanonicalRepository = "..\gc-image-processing"
)

$ErrorActionPreference = "Stop"
$CanonicalRoot = (Resolve-Path -LiteralPath $CanonicalRepository).Path

New-Item -ItemType Directory -Force proto\artifacts | Out-Null
New-Item -ItemType Directory -Force tests\fixtures\relay_v2 | Out-Null

Copy-Item -LiteralPath "$CanonicalRoot\proto\live_frame_relay_v2.proto" `
    -Destination proto\live_frame_relay_v2.proto
Copy-Item -LiteralPath "$CanonicalRoot\proto\artifacts\live_frame_relay_v2.desc" `
    -Destination proto\artifacts\live_frame_relay_v2.desc
Copy-Item -LiteralPath "$CanonicalRoot\proto\artifacts\live_frame_relay_v2.sha256" `
    -Destination proto\artifacts\live_frame_relay_v2.sha256
Copy-Item -LiteralPath "$CanonicalRoot\tests\fixtures\relay_v2\producer_hello.bin" `
    -Destination tests\fixtures\relay_v2\producer_hello.bin
Copy-Item -LiteralPath "$CanonicalRoot\tests\fixtures\relay_v2\producer_hello.sha256" `
    -Destination tests\fixtures\relay_v2\producer_hello.sha256

& .\scripts\generate_proto.ps1
if ($LASTEXITCODE -ne 0) {
    throw "Stream relay contract generation failed with exit code $LASTEXITCODE"
}
