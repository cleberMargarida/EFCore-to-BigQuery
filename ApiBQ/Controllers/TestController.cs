using ApiBQ.Data;
using BigQuery.EntityFramework.Core;
using BigQuery.EntityFramework.Core.Linq;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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
            MyEntity entity = new MyEntity {Id = 1, Name = "marvin", Update = new System.DateTime(2000, 1, 1) };
            context.MyEntities.Add(entity);
            entity.Update = System.DateTime.Now;
            context.MyEntities.Update(entity);
            context.MyEntities.Remove(entity);
            await context.MyEntities.SaveAsync();

            return await context.MyEntities.ToListAsync(); 
        }
    }
}
