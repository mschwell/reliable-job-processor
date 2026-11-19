using Microsoft.Extensions.Options;
using ReliableJobs.Api.Jobs;

namespace ReliableJobs.Api.Worker;

public sealed class JobWorker(
    JobRepository repository,
    IOptions<WorkerOptions> options,
    ILogger<JobWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        if (!settings.Enabled)
        {
            logger.LogInformation("Job worker is disabled in configuration.");
            return;
        }

        var pollMs = Math.Clamp(settings.PollIntervalMilliseconds, 100, 30_000);
        var leaseSeconds = Math.Clamp(settings.LeaseSeconds, 15, 300);
        logger.LogInformation("Worker started. Poll={PollMs}ms Lease={LeaseSeconds}s", pollMs, leaseSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await repository.RecoverExpiredAsync(stoppingToken);
                var job = await repository.ClaimNextAsync(leaseSeconds, stoppingToken);
                if (job is null)
                {
                    await Task.Delay(pollMs, stoppingToken);
                    continue;
                }

                logger.LogInformation("Claimed job {JobId}, kind={Kind}, attempt={Attempt}",
                    job.Id, job.Kind, job.Attempt);

                // This demo caps handler work at 5 seconds; no lease heartbeat is needed.
                // Production jobs can be long-running and would need periodic lease renewal.
                try
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    timeout.CancelAfter(TimeSpan.FromSeconds(Math.Max(5, leaseSeconds - 5)));
                    var output = await JobHandler.ExecuteAsync(job, timeout.Token);
                    await repository.RecordEffectAsync(job.Id, job.LeaseToken, output, stoppingToken);
                    var acknowledged = await repository.CompleteAsync(job.Id, job.LeaseToken, stoppingToken);
                    if (!acknowledged)
                        logger.LogWarning("Job {JobId} lost its lease; stale completion was rejected.", job.Id);
                    else
                        logger.LogInformation("Job {JobId} succeeded.", job.Id);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Let the lease expire; another worker will recover after restart.
                    throw;
                }
                catch (Exception ex)
                {
                    var message = ex.Message.Length > 1_000 ? ex.Message[..1_000] : ex.Message;
                    var acknowledged = await repository.FailAsync(
                        job.Id, job.LeaseToken, message,
                        JobPolicy.RetryDelaySeconds(job.Attempt), stoppingToken);
                    if (!acknowledged)
                        logger.LogWarning("Stale failure for job {JobId} was rejected.", job.Id);
                    else
                        logger.LogWarning(ex, "Job {JobId} attempt {Attempt} failed.", job.Id, job.Attempt);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Worker loop error; retrying after {PollMs}ms.", pollMs);
                try { await Task.Delay(pollMs, stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }
    }
}
