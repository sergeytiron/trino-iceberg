# PowerShell script to remove UTF-8 BOM from all source files recursively in the repository

$repoPath = "C:\Users\1ivi4\source\dotnet\trino-iceberg"

# Define source file extensions to process
$extensions = @("*.cs", "*.vb", "*.fs", "*.xaml", "*.xml", "*.json", "*.config", "*.ps1", "*.sh", "*.md", "*.csproj", "*.vbproj", "*.fsproj", "*.sln", "*.slnx", "*.props", "*.targets", "*.nuspec", "*.nupkg")

# Get all files recursively matching the extensions, excluding the git submodule
Get-ChildItem -Path $repoPath -Recurse -Include $extensions | Where-Object { $_.FullName -notlike "*\lib\trino-csharp-client\*" } | ForEach-Object {
    $file = $_.FullName
    try {
        # Read the file as bytes
        $bytes = [System.IO.File]::ReadAllBytes($file)
        # Check if it starts with UTF-8 BOM (EF BB BF)
        if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
            # Remove the BOM by taking bytes from index 3 onwards
            $newBytes = $bytes[3..($bytes.Length - 1)]
            # Write back without BOM
            [System.IO.File]::WriteAllBytes($file, $newBytes)
            Write-Host "Removed UTF-8 BOM from $file"
        }
    } catch {
        Write-Warning "Failed to process $file : $_"
    }
}

Write-Host "BOM removal process completed."