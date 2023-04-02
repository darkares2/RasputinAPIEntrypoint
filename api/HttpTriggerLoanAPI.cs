using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Text.Json;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Threading;
using System.Text.Json.Serialization;

namespace Rasputin.API
{
    public static class HttpTriggerLoanAPI
    { 
        [FunctionName("HttpTriggerLoanAPI")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "put", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            if (req.Method == "POST")
            {
                return await Post(req, log);
            }
            if (req.Method == "PUT")
            {
                return await Put(req, log);
            }
            else if (req.Method == "GET")
            {
                return await Get(req, log);
            }
            else
            {
                return new BadRequestObjectResult("Invalid request");
            }

        }

        private static async Task<IActionResult> Get(HttpRequest req, ILogger log)
        {
            string isbn = req.Query["isbn"];
            if (isbn != null)
            {
                return await GetLoanHistoryByISBN(isbn, req, log);
            }
            string idList = req.Query["ids"];

            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try {
                List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-loans", replyQueue);
                var cmd = new CmdLoan() { Command = "list_active_books_user", Parameter = idList };
                var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }) };
                log.LogInformation("Sending message to queue");
                await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }));
                return new OkObjectResult(await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log));
            } finally {
               await MessageHelper.DeleteTmpQueue(connectionString, replyQueue);
            }
        }

        private static async Task<IActionResult> GetLoanHistoryByISBN(string isbn, HttpRequest req, ILogger log)
        {
            log.LogInformation($"Get loan history for ISBN {isbn}");
            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try {
                List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-loans", replyQueue);
                var cmd = new CmdLoan() { Command = "list_loan_history_by_isbn", Parameter = isbn };
                var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }) };
                log.LogInformation("Sending message to queue");
                await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }));
                return new OkObjectResult(await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log));
            } finally {
               await MessageHelper.DeleteTmpQueue(connectionString, replyQueue);
            }
        }

        private static async Task<IActionResult> Post(HttpRequest req, ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var loan = JsonSerializer.Deserialize<Loans>(requestBody, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    NumberHandling = JsonNumberHandling.AllowReadingFromString
                });
            loan.Active = true;
            if (loan.LoanTimestamp == DateTime.MinValue)
            {
                loan.LoanTimestamp = DateTime.UtcNow;
            }
            log.LogInformation($"Loan: {loan.ISBN} {loan.UserId} {loan.LoanTimestamp} {loan.Active}");
            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try {
                List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-loans", replyQueue);
                var cmd = new CmdLoan() { Command = "loan", Loan = loan };
                var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }) };
                await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }));
                return new OkObjectResult(await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log));
            } finally {
               await MessageHelper.DeleteTmpQueue(connectionString, replyQueue);
            }
        }

        private static async Task<IActionResult> Put(HttpRequest req, ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var loan = JsonSerializer.Deserialize<Loans>(requestBody, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    NumberHandling = JsonNumberHandling.AllowReadingFromString
                });
            loan.Active = false;
            log.LogInformation($"Loan: {loan.ISBN} {loan.UserId} {loan.LoanTimestamp} {loan.Active}");
            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try {
                List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-loans", replyQueue);
                var cmd = new CmdLoan() { Command = "return", Loan = loan };
                var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }) };
                await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }));
                return new OkObjectResult(await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log));
            } finally {
               await MessageHelper.DeleteTmpQueue(connectionString, replyQueue);
            }
        }
    }
}
