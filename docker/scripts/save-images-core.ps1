Param([string]$Tag = (Get-Date -Format yyyyMMdd))
$images = @(
  "minio/minio:latest",
  "minio/mc:latest",
  "postgres:15",
  "tabulario/iceberg-rest:latest",
  "trinodb/trino:latest"
)
docker pull $images
$d = "pv-core-$Tag.tar"
docker save $images -o $d
Compress-Archive -Path $d -DestinationPath "$d.zip" -Force
Write-Host "Bundle created: $d.zip"