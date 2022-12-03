using System;

namespace BigQuery.EntityFramework.Core.DataAnnotations
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class KeyAttribute : Attribute
    {
    }
}
