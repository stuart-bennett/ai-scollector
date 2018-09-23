using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;

namespace Receiver
{
    internal class Program
    {
        private const int Port = 5000;
        private static readonly IPAddress s_localhost =
            IPAddress.Parse("127.0.0.1");

        internal static void Debug(string message, params object[] args)
        {
            Console.WriteLine($"[{DateTime.Now}] [{Thread.CurrentThread.ManagedThreadId}] " + message, args);
        }

        static void Main(string[] args)
        {
            IWebHost host = WebHost.CreateDefaultBuilder()
                .UseStartup<Startup>()
                .Build();

            host.Run();
        }
    }

    internal sealed class Worker
    {
        private static readonly TelemetryClient _telemetryClient =
            new TelemetryClient(CreateTelemetryConfiguration());

        private static TelemetryConfiguration CreateTelemetryConfiguration()
        {
            TelemetryConfiguration config = TelemetryConfiguration.CreateDefault();
            config.TelemetryChannel.DeveloperMode = true;
            return config;
        }

        public static void Work(Stream stream)
        {
            stream.Position = 0;
            using (GZipStream decompressionStream = new GZipStream(stream, CompressionMode.Decompress))
            using (StreamReader reader = new StreamReader(decompressionStream))
            {
                string json = string.Empty;
                json = reader.ReadToEnd();
                Console.WriteLine(json);
                IEnumerable<Item> items = JsonConvert.DeserializeObject<IEnumerable<Item>>(json);
                StringBuilder sb = new StringBuilder();
                foreach (Item item in items)
                {
                    _telemetryClient.GetMetric(item.Metric).TrackValue(item.Value);

                    sb.AppendFormat("{4} {0}: {1} ({3}){2}",
                        item.Metric,
                        item.Value,
                        Environment.NewLine,
                        string.Join(",", item.Tags.Select(x => x.Key + ": " + x.Value)),
                        Thread.CurrentThread.Name);
                }

                Program.Debug(sb.ToString());
            }
        }
    }

    public class SCollectorMiddleware
    {
        private readonly RequestDelegate _next;

        public SCollectorMiddleware(RequestDelegate next)
        {
            _next = next;
        }

        public async Task Run(HttpContext httpContext)
        {
            if (httpContext.Request.Headers["content-encoding"] == "gzip")
            {
                Stream s;
                if (httpContext.Request.ContentLength <= int.MaxValue)
                {
                    s = new MemoryStream((int)httpContext.Request.ContentLength);
                }
                else
                {
                    s = new MemoryStream();
                }

                httpContext.Request.Body.CopyTo(s);
                ThreadPool.QueueUserWorkItem<Stream>(Worker.Work, s, false);
            }
            else
            {
                using (StreamReader reader = new StreamReader(httpContext.Request.Body))
                {
                    // Console.WriteLine(await reader.ReadToEndAsync());
                }
            }

            await _next(httpContext);
            
            // openTSDB says that /api/put responses should return 204 if all
            // keys were stored successfully
            httpContext.Response.StatusCode = 204;
        }
    }

    internal sealed class Item
    {
        [JsonConstructor]
        public Item(string metric, int timestamp, object value, IDictionary<string, string> tags)
        {
            Metric = metric;
            Timestamp = timestamp;
            Value = value;
            Tags = tags;
        }

        public string Metric { get; }

        public int Timestamp { get; }

        public object Value { get; }

        public IDictionary<string, string> Tags { get; }
    }

    internal class Startup
    {
        public void Configure(IApplicationBuilder app)
        {
            app.Use(next => new SCollectorMiddleware(next).Run);
        }
    }
}
