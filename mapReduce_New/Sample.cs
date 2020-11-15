using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ServerlessMapReduce
{
    public struct NGramDetail {
        public NGramDetail(int nGramIn, int totalCountIn, Dictionary<String, int> countPerBookIn){
            nGram = nGramIn;
            totalCount = totalCountIn;
            if(countPerBookIn == null){
                countPerBook = new Dictionary<string, int>();
            } else {
                countPerBook = countPerBookIn;
            }
        }

        public int nGram { get; }
        public int totalCount { get; set;}
        public Dictionary<String, int> countPerBook { get; set; }

        public void addBookCount(string bookName, int count){
            totalCount = totalCount + count;
            countPerBook.Add(bookName, count);
        }

        public override string ToString()
        {
            string res = $"total_count: {totalCount}";
            foreach(var count in countPerBook){
                res += $", {count.Key}: {count.Value}";
            }
            return res;
        }

    }
    public static class Sample
    {
        private static readonly HttpClient _httpClient = HttpClientFactory.Create();

        [FunctionName(nameof(StartAsync))]
        public static async Task<HttpResponseMessage> StartAsync([HttpTrigger(AuthorizationLevel.Function, "post")]HttpRequestMessage req,
            [DurableClient] IDurableClient starter,
            ILogger log)
        {
            log.LogInformation(@"Starting orchestrator...");
            var newInstanceId = await starter.StartNewAsync(nameof(BeginMapReduce));
            //log.LogInformation($@"- Instance id: {newInstanceId}");

            return starter.CreateCheckStatusResponse(req, newInstanceId);
        }

        [FunctionName(nameof(BeginMapReduce))]
        public static async Task<string> BeginMapReduce([OrchestrationTrigger]IDurableOrchestrationContext context, ILogger log)
        {
            //retrieve storage Uri of each file via an Activity function
            Dictionary<string,string> books = new Dictionary<string, string>();
            var path = Directory.GetCurrentDirectory() +  "../../../../documents.txt";
            
            //string[] lines = File.ReadAllLines(@"documents.txt");
            string[] lines = File.ReadAllLines(path);

            foreach (string line in lines)
            {
                //todo: check
                var args =  line.Split("->");
                var key = args[0];
                var info = args[1];
                books.Add(key, info);
            }

            if (!context.IsReplaying)
            {
                log.LogInformation($@"{books.Count} file(s) found: {JsonConvert.SerializeObject(books)}");
                log.LogInformation(@"Creating mappers...");
                context.SetCustomStatus(new { status = @"Creating mappers", books });
            }
            //create mapper tasks which download and calculate avg speed from each csv file
            var tasks = new Task<KeyValuePair<string, int[]>>[books.Count];
            int index = 0;

            Console.WriteLine("Start reducer...");
            foreach(KeyValuePair<string, string> book in books){
                tasks[index++] = context.CallActivityAsync<KeyValuePair<string, int[]>>(
                    nameof(MapperAsync), book
                );
            }

            if (!context.IsReplaying)
            {
                log.LogInformation($@"Waiting for all {books.Count} mappers to complete...");
                context.SetCustomStatus(@"Waiting for mappers");
            }
            //wait all tasks done
            await Task.WhenAll(tasks);

            if (!context.IsReplaying)
            {
                log.LogInformation(@"Executing reducer...");
                context.SetCustomStatus(@"Executing reducer");
            }

            
            //create reducer activity function for aggregating result 
            var result = await context.CallActivityAsync<string>(
                nameof(Reducer),
                tasks.Select(task => task.Result));

            log.LogInformation($@"**FINISHED** Result: {result}");
            context.SetCustomStatus(null);

            return result;
        }

        /// <summary>
        /// Mapper Activity function to download and parse a CSV file
        /// </summary>
        [FunctionName(nameof(MapperAsync))]
        public static async Task<KeyValuePair<string, int[]>> MapperAsync(
            [ActivityTrigger] KeyValuePair<string, string> book,
            ILogger log)
        {
            log.LogInformation($@"Executing mapper for {book.Key} {book.Value}...");

//             var speedsByDayOfWeek = new double[7];
//             var numberOfLogsPerDayOfWeek = new int[7];

//             // download blob file
// #pragma warning disable IDE0067 // Dispose objects before losing scope
//             // Don't wrap in a Using because this was causing occasional ObjectDisposedException errors in v2 executions
//             var reader = new StreamReader(await _httpClient.GetStreamAsync(fileUri));
// #pragma warning restore IDE0067 // Dispose objects before losing scope

//             var lineText = string.Empty;
//             // read a line from NetworkStream
//             while (!reader.EndOfStream && (lineText = await reader.ReadLineAsync()) != null)
//             {
//                 // parse CSV format line
//                 var segdata = lineText.Split(',');

//                 // If it is header line or blank line, then continue
//                 if (segdata.Length != 17 || !int.TryParse(segdata[0], out var n))
//                 {
//                     continue;
//                 }

//                 // retrieve the value of pickup_datetime column
//                 var pickup_datetime = DateTime.Parse(segdata[1]);

//                 // retrieve the value of dropoff_datetime column
//                 var dropoff_datetime = DateTime.Parse(segdata[2]);

//                 // retrieve the value of trip_distance column
//                 var trip_distance = Convert.ToDouble(segdata[4]);

//                 if (trip_distance > 0)
//                 {
//                     double avgSpeed;
//                     try
//                     {
//                         // calculate avg speed
//                         avgSpeed = trip_distance / dropoff_datetime.Subtract(pickup_datetime).TotalHours;

//                         if (double.IsInfinity(avgSpeed) || double.IsNaN(avgSpeed))
//                         {
//                             continue;
//                         }
//                     }
// #pragma warning disable CA1031 // Do not catch general exception types
//                     catch (DivideByZeroException)
//                     {   // skip it
//                         continue;
//                     }
// #pragma warning restore CA1031 // Do not catch general exception types

//                     // sum of avg speed by each day of week
//                     speedsByDayOfWeek[(int)pickup_datetime.DayOfWeek] += avgSpeed;

//                     // number of trip by each day of week
//                     numberOfLogsPerDayOfWeek[(int)pickup_datetime.DayOfWeek]++;
//                 }
//             }

//             var results = numberOfLogsPerDayOfWeek
//                 .Select((val, idx) => val != 0 ? speedsByDayOfWeek[idx] / val : 0)
//                 .AsParallel()
//                 .ToList();

//             log.LogInformation($@"{fileUri} mapper complete. Returning {results.Count} result(s)");

            string bookName = book.Key;
            var args = book.Value.Split(" ");
            var url = args[0];
            var nGram = Int32.Parse(args[1]);
            var freq = Int32.Parse(args[2]);

            var minimal = new WebClient().DownloadString(url);
            // remove punctuation
            minimal = new string(minimal.Where(c => !char.IsPunctuation(c)).ToArray());
             //remove newline
            minimal = Regex.Replace(minimal,@"\t|\n|\r","");
             //remove double space 
            minimal = Regex.Replace(minimal,@"\s+"," ");

            minimal = minimal.ToLower();
            //int ngram = Int16.Parse(info.Split(" ")[1]);
            Dictionary<string, int> counts = new Dictionary<string,int>();

            string[] words = minimal.Split(" ");
            for(int i = 0; i + nGram - 1 < words.Length; i++)
            {
                string gram = "";
                for(int j = 0; j < nGram; j++){
                    gram += words[i+j];
                }

                // Add method throws an exception if the new key is already in the dictionary
                int val;
                counts[gram] = counts.TryGetValue(gram, out val) ? val + 1 : 1;
            }
            int count = 0;
            foreach(KeyValuePair<string,int> kvp in counts)
            {
                if (kvp.Value > freq)
                {
                    count++;
                }
            }
            Console.WriteLine(count);

            return new KeyValuePair<string, int[]>(
                bookName, 
                new int[]{nGram, count}
                );
        }

        /// <summary>
        /// Reducer Activity function for results aggregation
        /// </summary>
        [FunctionName(nameof(Reducer))]
        public static string Reducer(
            [ActivityTrigger] IEnumerable<KeyValuePair<string, int[]>> mapresults,
            ILogger log)
        {
            log.LogInformation(@"Reducing results...");


            var reduceResult = new Dictionary<int, NGramDetail>();
            // aggregate the result by each result produced via Mapper activity function.

            foreach(var book in mapresults){
                string bookName = book.Key;
                int nGram = book.Value[0];
                int count = book.Value[1];

                NGramDetail countDetail;
                if(!reduceResult.TryGetValue(nGram, out countDetail)){
                    countDetail = new NGramDetail(nGram, 0, null);
                } 
                countDetail.addBookCount(bookName, count);

                reduceResult[nGram] = countDetail;
                //Console.WriteLine("after add: "+reduceResult[nGram].nGram + " " + reduceResult[nGram].totalCount);
            }

            
            foreach(var elem in reduceResult){
                Console.WriteLine(elem.Key + " -> " + elem.Value.ToString());
            }

            // return aggregation result
            return reduceResult.ToString();
        }
    }
}