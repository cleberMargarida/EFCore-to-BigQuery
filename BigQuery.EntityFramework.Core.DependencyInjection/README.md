EntityFramework Core to BigQuery 
================

Installation
---
D.I extension package from NuGet, [BigQuery.EntityFramework.Core.DependencyInjection](https://nuget.org/packages/BigQuery.EntityFramework.Core.DependencyInjection)
```
PM> Install-Package BigQuery.EntityFramework.Core.DependencyInjection
```
Usage
---
```csharp
public void ConfigureServices(IServiceCollection services)
{
    services.AddBigQueryContext<BigQueryApplicationContext>(options => 
    {
        options.GoogleCredential = GoogleCredential.FromFile("client_secrets.json");
        options.ProjectId = "moonlit-text-367106";
    });
}
```
Author Info
---
linkedin: https://www.linkedin.com/in/cleber-margarida/

License
---
This library is under MIT License.
