using ApiBQ.Data;
using BigQuery.EntityFramework.Core.Linq;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
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
            context.MyEntities.Add(new MyEntity { Update = new System.DateTime(2000, 1, 1) });
            await context.MyEntities.SaveAsync();
            return await context.MyEntities.ToListAsync(); 
        }
    }
}
