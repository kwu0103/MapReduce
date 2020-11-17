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
        public static async Task<HttpResponseMessage> StartAsync([HttpTrigger(AuthorizationLevel.Function, "get")]HttpRequestMessage req,
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
            //var path = Directory.GetCurrentDirectory() +  "documents.txt";
            //var path = @"C:\Users\Kevin Wu\Documents\GitHub\serverless-mapreduce\documents.txt";
            //C:\Users\Kevin Wu\Documents\GitHub\serverless-mapreduce\obj\Debug\netcoreapp3.1\documents.txt
            string[] lines = File.ReadAllLines( @"C:\Users\Kevin Wu\Documents\GitHub\serverless-mapreduce\documents.txt");
            //string[] lines = File.ReadAllLines(path);
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
            var tasks = new Task<KeyValuePair<string,string>>[books.Count];
            int index = 0;

            Console.WriteLine("Start Mapper...");
            foreach(KeyValuePair<string, string> book in books){
                tasks[index++] = context.CallActivityAsync<KeyValuePair<string, string>>(
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
        public static async Task<KeyValuePair<string, string>> MapperAsync(
            [ActivityTrigger] KeyValuePair<string, string> book,
            ILogger log)
        {
            log.LogInformation($@"Executing mapper for {book.Key} {book.Value}...");


            string bookName = book.Key;
            var args = book.Value.Split(" ");
            var url = args[0];
            var nGram = Int32.Parse(args[1]);
            var freq = Int32.Parse(args[2]);

            var minimal = new WebClient().DownloadString(url);
            // remove punctuation
            minimal = Regex.Replace(minimal,@"[^\w\s]"," ");
             //remove newline
            minimal = Regex.Replace(minimal,@"\t|\n|\r"," ");
             //remove double space 
            minimal = Regex.Replace(minimal,@"\s+"," ");

            minimal = minimal.ToLower();

            Dictionary<string, int> counts = new Dictionary<string,int>();

            string[] words = minimal.Split(" ");
            for(int i = 0; i < words.Length - (nGram - 1); i++)
            {
                string gram = "";
                for(int j = 0; j < nGram; j++){
                    gram += words[i+j] + " ";
                }

                // Add method throws an exception if the new key is already in the dictionary
                int val;
                counts[gram] = counts.TryGetValue(gram, out val) ? val + 1 : 1;
            }
            var doc = "";
            foreach(KeyValuePair<string,int> kvp in counts)
            {
                if (kvp.Value < freq)
                {
                    counts.Remove(kvp.Key);
                }else
                {
                    doc += kvp.Key + "," + kvp.Value + "\n";
                }
            }
            //Console.WriteLine(doc);
            return new KeyValuePair<string, string>(bookName, doc);
        }

        /// <summary>
        /// Reducer Activity function for results aggregation
        /// </summary>
        [FunctionName(nameof(Reducer))]
        public static string Reducer(
            [ActivityTrigger] IEnumerable<KeyValuePair<string, string>> mapresults,
            ILogger log)
        {
            log.LogInformation(@"Reducing results...");


            string result="";
            // aggregate the result by each result produced via Mapper activity function.
            Dictionary<string, Dictionary<string,int>> allData = new Dictionary<string, Dictionary<string,int>>();
            foreach(var book in mapresults){
                string bookName = book.Key;
                string datas = book.Value;
                
                var words = datas.Split(new string[] {"\n"}, StringSplitOptions.RemoveEmptyEntries);
                if (!allData.ContainsKey(bookName)){
                    allData[bookName] = new Dictionary<string,int>();
                }
                
                foreach(var word in words){
                    var all = word.Split(",");
                    //Console.WriteLine(word);
                    //Console.WriteLine(all[0]);
                    //Console.WriteLine(all[1]);
                    string key = all[0];
                    int value = Int32.Parse(all[1]);
                    //Console.WriteLine(value);
                    allData[bookName][key] = value;
                }
                
            }
            
            var bookNames = allData.Keys.ToArray();
            var allNGrams = allData[bookNames[0]].Keys.ToArray();
            for (int i = 1; i < bookNames.Length; i++)
            {
                var ngram = allData[bookNames[i]].Keys.ToArray();
                allNGrams = allNGrams.Union(ngram).ToArray();
            }
            //var results = new Dictionary<string,dynamic>();
            foreach (var ngram in allNGrams)
            {
                result += ngram + "- ";
                int totalCount=0;
                foreach (var book in bookNames)
                {
                    try
                    {
                        totalCount += allData[book][ngram];
                        result += book + " ( " + allData[book][ngram] + " ) ";
                    }
                    catch (KeyNotFoundException)
                    {
                        totalCount += 0;
                        result += book + " ( " + 0 + " ) ";
                    }
                    
                }
                result += "totalCount"+ " ( " + totalCount + " )\n";
            }


        /*
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
        */
            // return aggregation result
            return result;
        }
    }
}