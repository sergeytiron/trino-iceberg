# Third-Party Libraries

This directory contains source code from third-party libraries that are included directly in the repository.

## Trino.Client

Source: https://github.com/trinodb/trino-csharp-client

The official Trino C# Client library is included here as source code because the NuGet packages are not yet published to the public NuGet.org feed. This allows the TrinoClient wrapper to build without requiring manual package management or private feeds.

**License**: Apache License 2.0 (see the original repository for details)

**Version**: Cloned from main branch on 2025-11-16

When the official packages are published to NuGet.org, this can be replaced with a standard PackageReference.
