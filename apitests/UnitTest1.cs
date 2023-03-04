using Xunit;
using Moq;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Internal;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace apitests;

public class UnitTest1
{
    [Fact]
    public async Task Test1Async()
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
        Assert.IsAssignableFrom<OkObjectResult>(response);
        var result = (OkObjectResult)response;
        Assert.Equal("Hello, Testing. This HTTP triggered function executed successfully.", result.Value);
    }
}