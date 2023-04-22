using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Collections.Generic;

namespace Rasputin.API
{
    public static class HttpTriggerIntegrationAPI
    {
        [FunctionName("HttpTriggerIntegrationAPI")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            var test = req.Query["test"];
            if (test.Equals("Full"))
            {
                return await FullIntegrationTest(req, log);
            }
            else
            {
                return new BadRequestObjectResult("Invalid request");
            }
        }

        private static async Task<IActionResult> FullIntegrationTest(HttpRequest req, ILogger log)
        {
            var testResult = new TestResult();
            testResult.ServiceCalls = new List<TestResult.ServiceCall>();
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try
            {
                log.LogInformation("Creating books");
                var book1 = new Books() { ISBN = "9783161484100", Title = "Test Book 1", Author = "Test Author 1", Price = (10.0).ToString(), PublicationDate = DateTime.Now };
                var book2 = new Books() { ISBN = "9783161484101", Title = "Test Book 2", Author = "Test Author 2", Price = (20.0).ToString(), PublicationDate = DateTime.Now };
                book1 = await CreateBook(book1, log, connectionString, replyQueue, testResult);
                book2 = await CreateBook(book2, log, connectionString, replyQueue, testResult);
                log.LogInformation("Creating users");
                var user1 = new Users() { Username = "testuser1", Password = "testpassword1", Email = "test1@mail.net" };
                var user2 = new Users() { Username = "testuser2", Password = "testpassword2", Email = "tst2@mail.net" };
                user1 = await CreateUser(user1, log, connectionString, replyQueue, testResult);
                user2 = await CreateUser(user2, log, connectionString, replyQueue, testResult);
                log.LogInformation("Creating loans");
                var loan1 = new Loans() { ISBN = book1.ISBN, UserId = user1.Id, LoanTimestamp = DateTime.Now, Active = true};
                var loan2 = new Loans() { ISBN = book2.ISBN, UserId = user2.Id, LoanTimestamp = DateTime.Now, Active = true};
                loan1 = await CreateLoan(loan1, log, connectionString, replyQueue, testResult);
                loan2 = await CreateLoan(loan2, log, connectionString, replyQueue, testResult);

                log.LogInformation("Getting loans");
                log.LogInformation("----------------------------------------");
                var history = await GetLoans(book1.ISBN, log, connectionString, replyQueue, testResult);
                ValidateUserHasBook(history, user1, log, testResult);
                history = await GetLoans(book2.ISBN, log, connectionString, replyQueue, testResult);
                ValidateUserHasBook(history, user2, log, testResult);
                log.LogInformation("----------------------------------------");

                log.LogInformation("Deleting loans");
                log.LogInformation("Deleting books");
                await DeleteBook(book1, log, connectionString, replyQueue, testResult);
                await DeleteBook(book2, log, connectionString, replyQueue, testResult);
                log.LogInformation("Deleting users");
                await DeleteUser(user1, log, connectionString, replyQueue, testResult);
                await DeleteUser(user2, log, connectionString, replyQueue, testResult);
                sw.Stop();
                testResult.DurationMs = sw.ElapsedMilliseconds;
                testResult.StatusCode = 200;
                testResult.ErrorMessage = "";
                return new OkObjectResult(JsonSerializer.Serialize(testResult, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error in HttpTriggerIntegrationAPI");
                sw.Stop();
                testResult.DurationMs = sw.ElapsedMilliseconds;
                testResult.StatusCode = 500;
                testResult.ErrorMessage = ex.Message;
                return new OkObjectResult(JsonSerializer.Serialize(testResult, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));
            }
            finally
            {
                await MessageHelper.DeleteTmpQueue(connectionString, replyQueue);
            }
        }

        private static void ValidateUserHasBook(LoanHistory history, Users user, ILogger log, TestResult testResult)
        {
            if (history.History.Length == 0)
            {
                log.LogError("User does not have book");
                testResult.ErrorMessage = "User does not have book";
                throw new Exception("User does not have book");
            }
            if (history.History[0].UserId != user.Id)
            {
                log.LogError("User does not have book");
                testResult.ErrorMessage = "User does not have book";
                throw new Exception("User does not have book");
            }
        }

        private static async Task<LoanHistory> GetLoans(string isbn, ILogger log, string connectionString, string replyQueue, TestResult testResult)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-loans", replyQueue);
            var cmd = new CmdLoan() { Command = "list_loan_history_by_isbn", Parameter = isbn };
            var message = new Message()
            {
                Headers = headers.ToArray(),
                Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            log.LogInformation("Sending message to queue");
            await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            }));
            var result = await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log);
            log.LogInformation($"History: {result}");
            sw.Stop();
            testResult.ServiceCalls.Add(new TestResult.ServiceCall()
            {
                ServiceName = "ms-loans",
                MethodName = "list_loan_history_by_isbn",
                DurationMs = sw.ElapsedMilliseconds
            });
            return JsonSerializer.Deserialize<LoanHistory>(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

        }

        private static async Task<Loans> CreateLoan(Loans loan, ILogger log, string connectionString, string replyQueue, TestResult testResult)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-loans", replyQueue);
            var cmd = new CmdLoan() { Command = "loan", Loan = loan };
            var message = new Message()
            {
                Headers = headers.ToArray(),
                Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            }));

            var result = await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log);
            sw.Stop();
            testResult.ServiceCalls.Add(new TestResult.ServiceCall()
            {
                ServiceName = "ms-loans",
                MethodName = "loan",
                DurationMs = sw.ElapsedMilliseconds
            });
            return JsonSerializer.Deserialize<Loans>(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
        }

        private static async Task<Users> DeleteUser(Users user, ILogger log, string connectionString, string replyQueue, TestResult testResult)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-users", replyQueue);
            var cmd = new CmdUser() { Command = "delete", User = user };
            var message = new Message()
            {
                Headers = headers.ToArray(),
                Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            }));
            var result = await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log);
            sw.Stop();
            testResult.ServiceCalls.Add(new TestResult.ServiceCall()
            {
                ServiceName = "ms-users",
                MethodName = "delete",
                DurationMs = sw.ElapsedMilliseconds
            });
            return JsonSerializer.Deserialize<Users>(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
        }

        private static async Task<Books> DeleteBook(Books book, ILogger log, string connectionString, string replyQueue, TestResult testResult)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-books", replyQueue);
            var cmd = new CmdUpdateBook() { Command = "delete", Book = book };
            var message = new Message()
            {
                Headers = headers.ToArray(),
                Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await MessageHelper.SendMessageAsync(Environment.GetEnvironmentVariable("rasputinServicebus"), "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            }));
            var result = await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log);
            sw.Stop();
            testResult.ServiceCalls.Add(new TestResult.ServiceCall()
            {
                ServiceName = "ms-books",
                MethodName = "delete",
                DurationMs = sw.ElapsedMilliseconds
            });
            return JsonSerializer.Deserialize<Books>(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
        }

        private static async Task<Users> CreateUser(Users user, ILogger log, string connectionString, string replyQueue, TestResult testResult)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-users", replyQueue);
            var cmd = new CmdUser() { Command = "create", User = user };
            var message = new Message()
            {
                Headers = headers.ToArray(),
                Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            }));
            var result = await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log);
            sw.Stop();
            testResult.ServiceCalls.Add(new TestResult.ServiceCall()
            {
                ServiceName = "ms-users",
                MethodName = "create",
                DurationMs = sw.ElapsedMilliseconds
            });
            return JsonSerializer.Deserialize<Users>(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
        }

        private static async Task<Books> CreateBook(Books book, ILogger log, string connectionString, string replyQueue, TestResult testResult)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-books", replyQueue);
            var cmd = new CmdUpdateBook() { Command = "create", Book = book };
            var message = new Message()
            {
                Headers = headers.ToArray(),
                Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await MessageHelper.SendMessageAsync(Environment.GetEnvironmentVariable("rasputinServicebus"), "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            }));
            var result = await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log);
            sw.Stop();
            testResult.ServiceCalls.Add(new TestResult.ServiceCall()
            {
                ServiceName = "ms-books",
                MethodName = "create",
                DurationMs = sw.ElapsedMilliseconds
            });
            return JsonSerializer.Deserialize<Books>(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
        }
    }
}
