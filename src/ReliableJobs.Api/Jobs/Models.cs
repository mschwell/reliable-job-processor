using System.Text.Json;

namespace ReliableJobs.Api.Jobs;

public sealed record EnqueueRequest(
    string Kind,
    JsonElement Payload,
    string? IdempotencyKey = null,
    DateTimeOffset? ScheduledFor = null,
    int MaxAttempts = 3);

public sealed record QueueResult(Guid Id, bool Existing, bool Conflict);

public sealed record ClaimedJob(
    Guid Id,
    string Kind,
    JsonElement Payload,
    int Attempt,
    int MaxAttempts,
    Guid LeaseToken);

public sealed record JobSummary(
    Guid Id,
    string Kind,
    string Status,
    int Attempts,
    int MaxAttempts,
    DateTimeOffset AvailableAt,
    DateTimeOffset CreatedAt,
    DateTimeOffset? CompletedAt,
    string? LastError,
    JsonElement? Output);

public sealed record JobAttempt(
    int AttemptNo,
    string Outcome,
    DateTimeOffset StartedAt,
    DateTimeOffset? CompletedAt,
    string? Error);

public sealed class WorkerOptions
{
    public bool Enabled { get; set; } = true;
    public int PollIntervalMilliseconds { get; set; } = 500;
    public int LeaseSeconds { get; set; } = 30;
}
