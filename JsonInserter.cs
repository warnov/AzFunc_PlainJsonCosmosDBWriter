
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Web.Http;

namespace CosmosDBJSonInserter
{
    public static class JsonInserter
    {
        [FunctionName("JsonInserter")]
        public static async Task<IActionResult> Run
            ([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]HttpRequest req,
            ILogger log,
            ExecutionContext context)
        {

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var cosmosDBEndpointUri = config["cosmosDBEndpointUri"];
            var cosmosDBPrimaryKey = config["cosmosDBPrimaryKey"];
            var cosmosDBName = config["cosmosDBName"];
            var cosmosDBCollectionName = config["cosmosDBCollectionName"];
            
            JArray data;
            string requestBody = new StreamReader(req.Body).ReadToEnd();
            try
            {
                data = (JArray)JsonConvert.DeserializeObject(requestBody);
            }
            catch(Exception exc)
            {
                log.LogError($"Problem with JSON input: {exc.Message}");
                return new BadRequestObjectResult(exc);
            }

            
            DocumentClient client;
            try
            {
                client = new DocumentClient(new Uri(cosmosDBEndpointUri), cosmosDBPrimaryKey);
                await client.CreateDatabaseIfNotExistsAsync(new Database { Id = cosmosDBName });
                await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(cosmosDBName),
                new DocumentCollection { Id = cosmosDBCollectionName });
            }
            catch (Exception exc)
            {
                log.LogError($"Problem communicating with CosmosDB: {exc.Message}");
                return new ExceptionResult(exc, true);
            }

            uint successfullInsertions = 0;
            foreach (var jobj in data)
            {
                try
                {
                    await client.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(cosmosDBName, cosmosDBCollectionName),
                        jobj);
                    successfullInsertions++;
                }
                catch(Exception exc)
                {
                    log.LogError($"Problem inserting document: {exc.Message}\n" +
                        $"Content: {jobj.ToString()}");                    
                }                
            }

            var endingMessage = $"{successfullInsertions} / {data.Count} " +
                $"documents inserted in CosmosDB";
            log.LogInformation(endingMessage);
            return new OkObjectResult(endingMessage);
        }
    }
}
