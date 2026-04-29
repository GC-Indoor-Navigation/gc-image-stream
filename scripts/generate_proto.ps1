New-Item -ItemType Directory -Force app\infrastructure\grpc\generated | Out-Null
New-Item -ItemType File -Force app\infrastructure\grpc\generated\__init__.py | Out-Null

.\.venv\Scripts\python-grpc-tools-protoc.exe `
  -I proto `
  --python_out=app\infrastructure\grpc\generated `
  --grpc_python_out=app\infrastructure\grpc\generated `
  proto\frame_ingest.proto `
  proto\stream_ingest.proto `
  proto\processing_relay.proto

Get-ChildItem app\infrastructure\grpc\generated -Filter *_pb2_grpc.py | ForEach-Object {
  $content = Get-Content $_.FullName -Raw
  $content = [regex]::Replace($content, '(?m)^import (.+_pb2) as ', 'from . import $1 as ')
  Set-Content $_.FullName $content
}
