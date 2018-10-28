using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlJobQueue));

        private static SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(30, 30);

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _options;
        public MySqlJobQueue(MySqlStorage storage, MySqlStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");

            _storage = storage;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");

            FetchedJob fetchedJob = null;
            MySqlConnection connection = null;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    connection = _storage.CreateAndOpenConnection();
                    
                    //var joinedQueues = string.Join(",", queues.Select(q => "'" + q.Replace("'", "''") + "'"));
                    /*var resource = ("JobQueue:" + joinedQueues);
                    if (resource.Length > 100)
                        resource = resource.Substring(0, 100);

                    using (new MySqlDistributedLock(_storage, resource, TimeSpan.FromSeconds(70)))*/
                    {
                        string token = Guid.NewGuid().ToString();

                        int nUpdated;


                        _semaphoreSlim.Wait(cancellationToken);

                        try
                        {
                            nUpdated = MySqlStorageConnection.AttemptActionReturnObject(() => connection.Execute(
                                "update JobQueue set FetchedAt = DATE_ADD(UTC_TIMESTAMP(), INTERVAL @timeout SECOND), FetchToken = @fetchToken " +
                                "where (FetchedAt is null or FetchedAt < UTC_TIMESTAMP()) " +
                                "   and Queue in @queues " +
                                // "ORDER BY FIELD(Queue, " + joinedQueues + "), JobId " +
                                "ORDER BY Priority DESC, JobId " +
                                "LIMIT 1;",
                                new
                                {
                                    queues = queues,
                                    timeout = 45, //_options.InvisibilityTimeout.Negate().TotalSeconds,
                                    fetchToken = token
                                },
                                commandTimeout: 15), 3);
                        }
                        finally
                        {
                            _semaphoreSlim.Release();
                        }

                        if (nUpdated != 0)
                        {
                            fetchedJob =
                                MySqlStorageConnection.AttemptActionReturnObject(() => connection
                                    .Query<FetchedJob>(
                                        "select Id, JobId, Queue " +
                                        "from JobQueue " +
                                        "where FetchToken = @fetchToken;",
                                        new
                                        {
                                            fetchToken = token
                                        },
                                        commandTimeout: 15)
                                    .SingleOrDefault(), 3);

                            if (fetchedJob != null)
                            {
                                nUpdated = MySqlStorageConnection.AttemptActionReturnObject(() => connection.Execute(
                                    "update JobQueue set FetchedAt = DATE_ADD(UTC_TIMESTAMP(), INTERVAL @timeout SECOND), FetchToken = @fetchToken " +
                                    "where FetchToken = @fetchToken;",
                                    new
                                    {
                                        timeout = _options.InvisibilityTimeout.TotalSeconds,
                                        fetchToken = token
                                    }, commandTimeout: 15), 5);

                                if (nUpdated == 0)
                                    fetchedJob = null;
                            }
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    Logger.ErrorException(ex.Message, ex);
                    throw;
                }
                finally
                {
                    _storage.ReleaseConnection(connection);
                }

                if (fetchedJob == null)
                {
                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (fetchedJob == null);

            return new MySqlFetchedJob(_storage, fetchedJob);
        }

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            Logger.TraceFormat("Enqueue JobId={0} Queue={1}", jobId, queue);
            connection.Execute("insert into JobQueue (JobId, Queue) values (@jobId, @queue)", new {jobId, queue});
        }
    }

    public static class Retry
    {
        public static void Do(
            Action action,
            TimeSpan retryInterval,
            int retryCount = 3)
        {
            Do<object>(() =>
            {
                action();
                return null;
            }, retryInterval, retryCount);
        }

        public static T Do<T>(
            Func<T> action,
            TimeSpan retryInterval,
            int retryCount = 3)
        {
            var exceptions = new List<Exception>();

            for (int retry = 0; retry < retryCount; retry++)
            {
                try
                {
                    return action();
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    Task.Delay(retryInterval).Wait();
                }
            }

            throw new AggregateException(exceptions);
        }
    }
}