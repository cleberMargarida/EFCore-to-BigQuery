EntityFramework Core to BigQuery 
================
BigQuery.EntityFramework.Core is C# Entity Framework Core Provider for [Google BigQuery](https://cloud.google.com/bigquery/).

Installation
---
package from NuGet, [BigQuery.EntityFramework.Core](https://nuget.org/packages/BigQuery.EntityFramework.Core)

```
PM> Install-Package BigQuery.EntityFramework.Core
```

Configuration
---
Define yours entities using the TableAttribute(**Required**) and KeyAttribute (**OPTIONAL**, will only be used when the entity Id is not specified during the queries).

e.g
```csharp
[Table(projectId: "moonlit-text-367106", dataset: "metadata", tableName: "myentity")]
public class MyEntity
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; }
}
```
Define a BqContext child class:

``` csharp
public class BigQueryApplicationContext : BqContext
{
    public BigQueryApplicationContext(BigQueryContextBuilder builder) : base(builder) 
    {
    }
    public BqSet<MyEntity> MyEntities { get; set; }
}
```
Operations
---
## Insert
``` csharp
context.MyEntities.Add(myEntity)
```
## Update
``` csharp
//Update. when key attribute is defined.

context.MyEntities.Update(myEntity)
```
``` csharp
//Update all following an expression. key attribute is not necessary.

context.MyEntities.Update(x => x.Id == 1, myEntity);
```
``` csharp
//Update all following a property constant. key attribute is not necessary.

context.MyEntities.Update(x => x.Id, myEntity);
```
## Delete
``` csharp
//follow the same as Update

context.MyEntities.Remove(myEntity);//for this key is required.
context.MyEntities.Remove(x => x.Id);
context.MyEntities.Remove(x => x.Id == 1);
```
----
#### All these operations will queue up queries to be executed in the big query. For these queries to be dequeued and executed, we need to call the Save method
``` csharp
await context.MyEntities.SaveAsync();
```
Queries
---
All BigQuery functions is under BqFunc.
<br>
*note*: BigQuery.EntityFramework.Core is a Entity Provider but is not IQueryable. It's constraint is old good method chain.
#### The rule follows:
```
From((+TableDecorate)+Flatten) -> Join -> Where -| -> OrderBy(ThenBy) -> Select ->                     | -> Limit -> IgnoreCase
                                                 | -> Select | -> GroupBy -> Having -> OrderBy(ThenBy) | -> IgnoreCase
                                                             | -> OrderBy(ThenBy) ->                   |
```
Run/RunAsync - Execute query with QueryResponse.
<br> 
ToList/ToListAsync - Execute query and return rows.
<br> 
AsEnumerable - Execute query and return rows, it's deferred(but resultset is not streaming).
<br> 
ToString - Build query string. It no needs network connection and BigqueryService.
<br> 
Into - Query as Subquery(same as From(query))

Author Info
---
linkedin: https://www.linkedin.com/in/cleber-margarida/

License
---
This library is under MIT License.
