using System.Collections.Generic;
using System;
using System.Net.Http;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace mapreduce
{
    public static class serverless
    {
        [FunctionName("serverless")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var input = context.GetInput<string>();
            //Console.WriteLine(input);
            var response = new WebClient().DownloadString(input);
            Console.WriteLine(response);
            var outputs = new List<string>();
            
            //Console.writeLine(input);
            //var input = context.GetInput<string>();
           // var url = input["url"].ToString();
                // Replace "hello" with the name of your Durable Activity Function.
            
            outputs.Add(await context.CallActivityAsync<string>("serverless_Hello", "Tokyo"));
            //outputs.Add(await context.CallActivityAsync<string>("serverless_Hello", "Seattle"));
            //outputs.Add(await context.CallActivityAsync<string>("serverless_Hello", "London"));

            // returns ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
            return outputs;
        }

        [FunctionName("serverless_Hello")]
        public static string SayHello([ActivityTrigger] string name, ILogger log)
        {
            log.LogInformation($"Saying hello to {name}.");
            return $"Hello {name}!";
        }

        [FunctionName("serverless_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            Uri url = new Uri( "https://www.gutenberg.org/files/63713/63713-0.txt");
            //var response = new WebClient().DownloadString("https://www.gutenberg.org/files/63713/63713-0.txt");
            Console.WriteLine(url);

            string instanceId = await starter.StartNewAsync("serverless", url);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}