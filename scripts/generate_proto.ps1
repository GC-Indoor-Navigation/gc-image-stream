New-Item -ItemType Directory -Force app\infrastructure\grpc\generated | Out-Null
New-Item -ItemType File -Force app\infrastructure\grpc\generated\__init__.py | Out-Null

.\.venv\Scripts\python-grpc-tools-protoc.exe `
  -I proto `
  --python_out=app\infrastructure\grpc\generated `
  --grpc_python_out=app\infrastructure\grpc\generated `
  proto\frame_ingest.proto `
  proto\processing_relay.proto `
  proto\live_frame_relay_v2.proto

Get-ChildItem app\infrastructure\grpc\generated -Filter *_pb2_grpc.py | ForEach-Object {
  $content = Get-Content $_.FullName -Raw
  $content = [regex]::Replace($content, '(?m)^import (.+_pb2) as ', 'from . import $1 as ')
  Set-Content -LiteralPath $_.FullName -Value $content -NoNewline
}
