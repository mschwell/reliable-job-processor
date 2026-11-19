using System.Text.Json;

namespace ReliableJobs.Api.Jobs;

public static class JobPolicy
{
    public static int RetryDelaySeconds(int attempt)
        => Math.Min(60, 1 << Math.Clamp(attempt, 1, 6));

    public static string? Validate(EnqueueRequest request)
    {
        if (request.Kind is not ("sum" or "flaky" or "delay"))
            return "kind must be sum, flaky, or delay.";
        if (request.Payload.ValueKind != JsonValueKind.Object)
            return "payload must be a JSON object.";
        if (request.MaxAttempts is < 1 or > 10)
            return "maxAttempts must be between 1 and 10.";
        if (request.IdempotencyKey is { Length: > 100 } ||
            (request.IdempotencyKey is not null && string.IsNullOrWhiteSpace(request.IdempotencyKey)))
            return "idempotencyKey must be a nonblank string of at most 100 characters.";
        if (request.ScheduledFor > DateTimeOffset.UtcNow.AddDays(7))
            return "scheduledFor must be within the next 7 days.";

        switch (request.Kind)
        {
            case "sum":
                if (!request.Payload.TryGetProperty("numbers", out var numbers) ||
                    numbers.ValueKind != JsonValueKind.Array || numbers.GetArrayLength() is < 1 or > 50)
                    return "sum requires numbers: an array of 1 to 50 numbers.";
                foreach (var number in numbers.EnumerateArray())
                {
                    if (number.ValueKind != JsonValueKind.Number ||
                        !number.TryGetDecimal(out var value) || (value < -1_000_000m || value > 1_000_000m))
                        return "each number must be a decimal between -1000000 and 1000000.";
                }
                break;
            case "flaky":
                if (!request.Payload.TryGetProperty("failUntilAttempt", out var failUntil) ||
                    failUntil.ValueKind != JsonValueKind.Number ||
                    !failUntil.TryGetInt32(out var failureCount) || failureCount is < 0 or > 9)
                    return "flaky requires failUntilAttempt: an integer between 0 and 9.";
                break;
            case "delay":
                if (!request.Payload.TryGetProperty("milliseconds", out var delay) ||
                    delay.ValueKind != JsonValueKind.Number ||
                    !delay.TryGetInt32(out var ms) || ms is < 0 or > 5_000)
                    return "delay requires milliseconds: an integer between 0 and 5000.";
                break;
        }
        return null;
    }
}
