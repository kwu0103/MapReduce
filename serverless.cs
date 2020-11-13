using System.Collections.Generic;
using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
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
            // get input from start
            var input = context.GetInput<Dictionary<string, string>>();
            //get keys (documents)
            var arrayofKeys = input.Keys.ToArray();
            var size = arrayofKeys.Length;
            //Dictionary<string, string> counts = new Dictionary<string,string>();
            /*
            string[,] info = new string[size,3];
            for (var i = 0; i < size; i++){
                //var minimal = new WebClient().DownloadString(input[0])
                //txts[i] = 
                string tmp = input[arrayofKeys[i]].Split(" ")[0];
                var minimal = new WebClient().DownloadString(tmp);
                // remove punctuation
                minimal = new string(minimal.Where(c => !char.IsPunctuation(c)).ToArray());
                 //remove newline
                minimal = Regex.Replace(minimal,@"\t|\n|\r","");
                 //remove double space 
                minimal = Regex.Replace(minimal,@"\s+"," ");
                info[i,0] = minimal.ToLower();
                for(var j = 1; j < 3; j++){
                    info[i,j] = input[arrayofKeys[i]].Split(" ")[j];
                }
            }
            */
            var parallelTasks = new List<Task<string>>();
            for (int i = 0; i < size; i++) {
                var key = arrayofKeys[i];
                Task<string> tmp = context.CallActivityAsync<string>(nameof(MapperAsync),input[key]);
                parallelTasks.Add(tmp);
            }
            await Task.WhenAll(parallelTasks);

            

            
            /*
            //test n-gram
            var n = 2;
            //get url
            var input = context.GetInput<string>();
            //get-txt
            var response = new WebClient().DownloadString(input);
            // remove punctuation
            string minimal = new string(response.Where(c => !char.IsPunctuation(c)).ToArray());
            //remove newline
            minimal = Regex.Replace(minimal,@"\t|\n|\r","");
            //remove double space 
            minimal = Regex.Replace(minimal,@"\s+"," ");
            //Console.WriteLine(minimal);

            Dictionary<string,int> counts = new Dictionary<string, int>();
            
            string[] words = minimal.Split(" ");
            for(int i = 0; i < words.Length - (n-1); i++)
            {
                
                var grams = $"\n{words[i]} {words[i + 1]}";

                // Add method throws an exception if the new key is already in the dictionary
                try
                {
                    counts.Add(grams,1);
                }
                catch (ArgumentException)
                {
                    counts[grams] += 1;
                }
                
            }

            foreach(KeyValuePair<string,int> kvp in counts)
            {
                if (kvp.Value >= 2)
                {
                    Console.WriteLine("Key: {0}, Value:{1}", kvp.Key, kvp.Value);
                }
            }
*/
            
            
            //Console.writeLine(input);
            //var input = context.GetInput<string>();
           // var url = input["url"].ToString();
                // Replace "hello" with the name of your Durable Activity Function.
            var outputs = new List<string>();
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
        [FunctionName("MapperAsync")]
        public static async Task<string> MapperAsync ([ActivityTrigger] string info)
        {
            string tmp = info.Split(" ")[0];
            var minimal = new WebClient().DownloadString(tmp);
            // remove punctuation
            minimal = new string(minimal.Where(c => !char.IsPunctuation(c)).ToArray());
             //remove newline
            minimal = Regex.Replace(minimal,@"\t|\n|\r","");
             //remove double space 
            minimal = Regex.Replace(minimal,@"\s+"," ");

            minimal = minimal.ToLower();
            //int ngram = Int16.Parse(info.Split(" ")[1]);
            int frequency = Int16.Parse(info.Split(" ")[2]);
            Dictionary<string, int> counts = new Dictionary<string,int>();

            string[] words = minimal.Split(" ");
            for(int i = 0; i < words.Length - 1; i++)
            {
                
                var grams = $"\n{words[i]} {words[i + 1]}";

                // Add method throws an exception if the new key is already in the dictionary
                try
                {
                    counts.Add(grams,1);
                }
                catch (ArgumentException)
                {
                    counts[grams] += 1;
                }
                
            }
            string response = "";
            foreach(KeyValuePair<string,int> kvp in counts)
            {
                if (kvp.Value >= frequency)
                {
                    response += kvp.Key + " " + kvp.Value + "\n";
                    
                }
            }
            Console.WriteLine(response);


            return response;
        }

        [FunctionName("serverless_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.

            Dictionary<string,string> list = new Dictionary<string, string>();
            string[] lines = File.ReadAllLines(@"C:\Users\Kevin Wu\Documents\GitHub\serverless-mapreduce\documents.txt");
            foreach (string line in lines)
            {
                var key = line.Split("->")[0];
                var value = line.Split("->")[1];
                list.Add(key,value);
            }
            //Uri url = new Uri( "https://www.gutenberg.org/files/63713/63713-0.txt");
            //var response = new WebClient().DownloadString("https://www.gutenberg.org/files/63713/63713-0.txt");
            //Console.WriteLine(url);

            string instanceId = await starter.StartNewAsync("serverless",list);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}