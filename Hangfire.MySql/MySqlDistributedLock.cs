using System;
using System.Data;
using System.Threading;
using Dapper;
using Hangfire.Logging;

namespace Hangfire.MySql
{
    public class MySqlDistributedLock : IDisposable, IComparable
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly string _resource;
        private readonly TimeSpan _timeout;
        private readonly MySqlStorage _storage;
        private readonly DateTime _start;
        private readonly CancellationToken _cancellationToken;

        private const int DelayBetweenPasses = 100;

        public MySqlDistributedLock(MySqlStorage storage, string resource, TimeSpan timeout)
            : this(resource, timeout)
        {
            _storage = storage;
        }

        public MySqlDistributedLock(string resource, TimeSpan timeout)
            : this(resource, timeout, new CancellationToken())
        {
        }

        public MySqlDistributedLock(string resource, TimeSpan timeout, CancellationToken cancellationToken)
        {
            Logger.TraceFormat("MySqlDistributedLock resource={0}, timeout={1}", resource, timeout);

            _resource = resource;
            _timeout = timeout;
            _cancellationToken = cancellationToken;
            _start = DateTime.UtcNow;
        }

        public string Resource {
            get { return _resource; }
        }

        private int AcquireLock(string resource, TimeSpan timeout)
        {

            return MySqlStorageConnection.AttemptActionReturnObject(() =>

                _storage.UseConnection(connection =>
                    connection
                        .Execute(
                            "INSERT INTO DistributedLock (Resource, CreatedAt) " +
                            "  SELECT @resource, @now " +
                            "  FROM dual " +
                            "  WHERE NOT EXISTS ( " +
                            "  		SELECT * FROM DistributedLock " +
                            "     	WHERE Resource = @resource " +
                            "       AND CreatedAt > @expired)",
                            new
                            {
                                resource,
                                now = DateTime.UtcNow,
                                expired = DateTime.UtcNow.Add(timeout.Negate())
                            }))

                );
        }

        public void Dispose()
        {
            Release();

        }

        internal MySqlDistributedLock Acquire()
        {
            Logger.TraceFormat("Acquire resource={0}, timeout={1}", _resource, _timeout);

            int insertedObjectCount;
            do
            {
                _cancellationToken.ThrowIfCancellationRequested();

                insertedObjectCount = AcquireLock(_resource, _timeout);

                if (ContinueCondition(insertedObjectCount))
                {
                    _cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    _cancellationToken.ThrowIfCancellationRequested();
                }
            } while (ContinueCondition(insertedObjectCount));

            if (insertedObjectCount == 0)
            {
                throw new MySqlDistributedLockException("cannot acquire lock");
            }
            return this;
        }

        private bool ContinueCondition(int insertedObjectCount)
        {
            return insertedObjectCount == 0 && _start.Add(_timeout) > DateTime.UtcNow;
        }

        internal void Release()
        {
            Logger.TraceFormat("Release resource={0}", _resource);

            _storage.UseConnection(connection =>
                connection
                .Execute(
                    "DELETE FROM DistributedLock  " +
                    "WHERE Resource = @resource",
                    new
                    {
                        resource = _resource
                    }));
        }

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;

            var mySqlDistributedLock = obj as MySqlDistributedLock;
            if (mySqlDistributedLock != null)
                return string.Compare(this.Resource, mySqlDistributedLock.Resource, StringComparison.InvariantCultureIgnoreCase);
            
            throw new ArgumentException("Object is not a mySqlDistributedLock");
        }
    }
}