using ApiBQ.Data;
using BigQuery.EntityFramework.Core;
using BigQuery.EntityFramework.Core.Linq;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static BigQuery.EntityFramework.Core.BqFunc;

namespace ApiBQ.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TestController : ControllerBase
    {
        private readonly BigQueryApplicationContext context;
        public TestController(BigQueryApplicationContext context)
        {
            this.context = context;
        }

        [HttpGet]
        public async Task<IEnumerable<MyEntity>> GetAsync()
        {
            MyEntity entity = new MyEntity
            {
                Id = 1,
                Name = "marvin",
                Update = new DateTime(2021, 1, 1),
                Test = new Test
                {
                    TestName = "teste name",
                    TestId = 1,
                    TestIntern = new TestIntern
                    {
                        Description = "cleber"
                    }
                }
            };
            context.MyEntities.Add(entity);
            await context.MyEntities.SaveAsync();

            entity.Update = System.DateTime.Now;
            context.MyEntities.Update(entity);
            await context.MyEntities.SaveAsync();

            context.MyEntities.Remove(entity);
            await context.MyEntities.SaveAsync();

            var query = context.MyEntities
                .With(x => x.Where(x => x.Id > 0).Select(x => new { x.Name, x.Update }))
                .Select(x => ToJsonString(x))
                .ToList();

            return await context.MyEntities.ToListAsync(); 
        }
    }
}
