using System.Collections.Generic;

internal class TestResult {
    public class ServiceCall {
        public string ServiceName { get; set; }
        public string MethodName { get; set; }
        public long DurationMs { get; set; }
    }
    public List<ServiceCall> ServiceCalls { get; set; }
    public long DurationMs { get; set; }
    public int StatusCode { get; set; }
    public string ErrorMessage { get; set; }
}