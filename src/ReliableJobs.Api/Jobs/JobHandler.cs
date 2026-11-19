using System.Text.Json;

namespace ReliableJobs.Api.Jobs;

// All demo handlers have bounded execution and only write to a synthetic DB sink.
public static class JobHandler
{
    public static async Task<string> ExecuteAsync(ClaimedJob job, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return job.Kind switch
        {
            "sum" => JsonSerializer.Serialize(new
            {
                sum = job.Payload.GetProperty("numbers").EnumerateArray()
                    .Sum(x => x.GetDecimal())
            }),
            "flaky" => Flaky(job),
            "delay" => await DelayAsync(job, cancellationToken),
            _ => throw new NotSupportedException($"Unrecognized job kind: {job.Kind}")
        };
    }

    private static string Flaky(ClaimedJob job)
    {
        var failUntil = job.Payload.GetProperty("failUntilAttempt").GetInt32();
        if (job.Attempt <= failUntil)
            throw new InvalidOperationException($"Intentional demo failure on attempt {job.Attempt}.");
        return JsonSerializer.Serialize(new { message = "Success after retry", attempt = job.Attempt });
    }

    private static async Task<string> DelayAsync(ClaimedJob job, CancellationToken cancellationToken)
    {
        var ms = job.Payload.GetProperty("milliseconds").GetInt32();
        await Task.Delay(ms, cancellationToken);
        return JsonSerializer.Serialize(new { waitedMilliseconds = ms });
    }
}
