using Xunit;
using Moq;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Internal;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;

namespace apitests;

public class UnitTestBookAPI
{
    [Fact]
    public async Task UnitTestInvalidRequest()
    {
        var logMock = new Mock<ILogger>();
        var request = new DefaultHttpRequest(new DefaultHttpContext())
            {
                Query = new QueryCollection
                (
                    new Dictionary<string, StringValues>()
                    {
                { "name", "Testing" }
                    }
                )
            };
        var response = await Rasputin.API.HttpTriggerBookAPI.Run(request, logMock.Object);
        Assert.IsAssignableFrom<BadRequestObjectResult>(response);
        var result = (BadRequestObjectResult)response;
        Assert.Equal("Invalid request", result.Value);
    }
}