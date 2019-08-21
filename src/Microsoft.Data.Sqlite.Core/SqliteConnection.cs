// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite.Properties;
using Microsoft.Data.Sqlite.Utilities;
using SQLitePCL;

using static SQLitePCL.raw;

namespace Microsoft.Data.Sqlite
{
    /// <summary>
    ///     Represents a connection to a SQLite database.
    /// </summary>
    public partial class SqliteConnection : DbConnection
    {
        internal string _defaultPoolConnectionString;

        internal SqliteConnectionPool _defaultPool;

        internal static Dictionary<string, SqliteConnectionPool> _connections = new Dictionary<string, SqliteConnectionPool>();

        internal const string MainDatabaseName = "main";

        internal readonly List<WeakReference<SqliteCommand>> _commands = new List<WeakReference<SqliteCommand>>();

        internal readonly Dictionary<string, (object state, strdelegate_collation collation)> _collations
            = new Dictionary<string, (object, strdelegate_collation)>(StringComparer.OrdinalIgnoreCase);

        internal readonly Dictionary<(string name, int arity), (int flags, object state, delegate_function_scalar func)> _functions
            = new Dictionary<(string, int), (int, object, delegate_function_scalar)>(FunctionsKeyComparer.Instance);

        internal readonly Dictionary<(string name, int arity), (int flags, object state, delegate_function_aggregate_step func_step, delegate_function_aggregate_final func_final)> _aggregates
            = new Dictionary<(string, int), (int, object, delegate_function_aggregate_step, delegate_function_aggregate_final)>(FunctionsKeyComparer.Instance);

        internal readonly HashSet<(string file, string proc)> _extensions = new HashSet<(string, string)>();

        private string _connectionString;
        private ConnectionState _state;
        private int _defaultTimeout = 30;
        internal bool _extensionsEnabled;

        static SqliteConnection()
        {
            BundleInitializer.Initialize();
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="SqliteConnection" /> class.
        /// </summary>
        public SqliteConnection()
        {
        }

        /// <summary>
        /// Frees all open connections from the pool.
        /// </summary>
        public static void FreeHandles()
        {
            lock (_connections)
            {
                foreach (var pool in _connections.Values)
                {
                    pool.Dispose();
                }

                _connections.Clear();
            }
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="SqliteConnection" /> class.
        /// </summary>
        /// <param name="connectionString">The string used to open the connection.</param>
        /// <seealso cref="SqliteConnectionStringBuilder" />
        public SqliteConnection(string connectionString)
            => ConnectionString = connectionString;

        private SqliteConnectionHandle _handle;

        private SqliteConnectionHandle AcquireHandle()
        {
            if (_handle != null)
            {
                return _handle;
            }

            if (_connectionString == null)
            {
                throw new InvalidOperationException(Resources.OpenRequiresSetConnectionString);
            }

            // Can't reuse an in-memory database connection
            if (_connectionString.IndexOf(":memory:") < 0)
            {
                SqliteConnectionPool pool;

                if (_defaultPool != null && _connectionString == _defaultPoolConnectionString)
                {
                    pool = _defaultPool;
                }
                else if (!_connections.TryGetValue(_connectionString, out pool))
                {
                    _connections[_connectionString] = pool = new SqliteConnectionPool(64);

                    _defaultPoolConnectionString = _connectionString;
                    _defaultPool = pool;
                }

                _handle = pool.Rent();
            }

            if (_handle == null)
            {
                _handle = new SqliteConnectionHandle();
            }

            _handle.Initialize(this);

            _handle._connectionString = _connectionString;
            _handle.ConnectionOptions = new SqliteConnectionStringBuilder(_connectionString);

            return _handle;
        }

        internal SqliteConnectionStringBuilder ConnectionOptions => AcquireHandle().ConnectionOptions;

        private void ReleaseHandle()
        {
            if (_handle == null)
            {
                return;
            }

            _handle.Release();

            if (_defaultPool != null && _connectionString == _defaultPoolConnectionString)
            {
                _defaultPool.Return(_handle);
                _handle = null;
            }
            else if (_connections.TryGetValue(_connectionString, out var pool))
            {
                pool.Return(_handle);
                _handle = null;
            }
            else
            {
                _handle.Dispose();
            }
        }

        /// <summary>
        ///     Gets a handle to underlying database connection.
        /// </summary>
        /// <value>A handle to underlying database connection.</value>
        /// <seealso href="http://sqlite.org/c3ref/sqlite3.html">Database Connection Handle</seealso>
        public virtual sqlite3 Handle
            => AcquireHandle()._db;

        /// <summary>
        ///     Gets or sets a string used to open the connection.
        /// </summary>
        /// <value>A string used to open the connection.</value>
        /// <seealso cref="SqliteConnectionStringBuilder" />
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (_state != ConnectionState.Closed)
                {
                    throw new InvalidOperationException(Resources.ConnectionStringRequiresClosedConnection);
                }

                _connectionString = value;
            }
        }

        /// <summary>
        ///     Gets the name of the current database. Always 'main'.
        /// </summary>
        /// <value>The name of the current database.</value>
        public override string Database
            => MainDatabaseName;

        /// <summary>
        ///     Gets the path to the database file. Will be absolute for open connections.
        /// </summary>
        /// <value>The path to the database file.</value>
        public override string DataSource
        {
            get
            {
                AcquireHandle();

                string dataSource = null;
                if (State == ConnectionState.Open)
                {
                    dataSource = sqlite3_db_filename(_handle._db, MainDatabaseName).utf8_to_string();
                }

                return dataSource ??_handle.ConnectionOptions.DataSource;
            }
        }

        /// <summary>
        ///     Gets or sets the default <see cref="SqliteCommand.CommandTimeout" /> value for commands created using
        ///     this connection. This is also used for internal commands in methods like
        ///     <see cref="BeginTransaction()" />.
        /// </summary>
        /// <value>The default <see cref="SqliteCommand.CommandTimeout" /> value.</value>
        public virtual int DefaultTimeout
        {
            get => _defaultTimeout;
            set
            {
                _defaultTimeout = value;

                if (_handle != null)
                {
                    _handle._defaultTimeout = value;
                }
            }
        }

        /// <summary>
        ///     Gets the version of SQLite used by the connection.
        /// </summary>
        /// <value>The version of SQLite used by the connection.</value>
        public override string ServerVersion
            => sqlite3_libversion().utf8_to_string();

        /// <summary>
        ///     Gets the current state of the connection.
        /// </summary>
        /// <value>The current state of the connection.</value>
        public override ConnectionState State
            => _state;

        /// <summary>
        ///     Gets the <see cref="DbProviderFactory" /> for this connection.
        /// </summary>
        /// <value>The <see cref="DbProviderFactory" />.</value>
        protected override DbProviderFactory DbProviderFactory
            => SqliteFactory.Instance;

        /// <summary>
        ///     Gets or sets the transaction currently being used by the connection, or null if none.
        /// </summary>
        /// <value>The transaction currently being used by the connection.</value>
        protected internal virtual SqliteTransaction Transaction { get; set; }

        /// <summary>
        ///     Opens a connection to the database using the value of <see cref="ConnectionString" />. If
        ///     <c>Mode=ReadWriteCreate</c> is used (the default) the file is created, if it doesn't already exist.
        /// </summary>
        /// <exception cref="SqliteException">A SQLite error occurs while opening the connection.</exception>
        public override void Open()
        {
            AcquireHandle();

            _state = ConnectionState.Open;

            try
            {
                _handle.Open();
            }
            catch
            {
                _state = ConnectionState.Closed;
                throw;
            }

            OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Open));
        }

        /// <summary>
        ///     Closes the connection to the database. Open transactions are rolled back.
        /// </summary>
        public override void Close()
        {
            _state = ConnectionState.Closed;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
        }

        /// <summary>
        ///     Releases any resources used by the connection and closes it.
        /// </summary>
        /// <param name="disposing">
        ///     true to release managed and unmanaged resources; false to release only unmanaged resources.
        /// </param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }

            ReleaseHandle();

            base.Dispose(disposing);
        }

        /// <summary>
        ///     Creates a new command associated with the connection.
        /// </summary>
        /// <returns>The new command.</returns>
        /// <remarks>
        ///     The command's <seealso cref="SqliteCommand.Transaction" /> property will also be set to the current
        ///     transaction.
        /// </remarks>
        public new virtual SqliteCommand CreateCommand()
            => new SqliteCommand
            {
                Connection = this,
                CommandTimeout = DefaultTimeout,
                Transaction = Transaction
            };

        /// <summary>
        ///     Creates a new command associated with the connection.
        /// </summary>
        /// <returns>The new command.</returns>
        protected override DbCommand CreateDbCommand()
            => CreateCommand();

        internal void AddCommand(SqliteCommand command)
            => _commands.Add(new WeakReference<SqliteCommand>(command));

        internal void RemoveCommand(SqliteCommand command)
        {
            for (var i = _commands.Count - 1; i >= 0; i--)
            {
                if (!_commands[i].TryGetTarget(out var item)
                    || item == command)
                {
                    _commands.RemoveAt(i);
                }
            }
        }

        /// <summary>
        ///     Create custom collation.
        /// </summary>
        /// <param name="name">Name of the collation.</param>
        /// <param name="comparison">Method that compares two strings.</param>
        public virtual void CreateCollation(string name, Comparison<string> comparison)
            => CreateCollation(name, null, comparison != null ? (_, s1, s2) => comparison(s1, s2) : (Func<object, string, string, int>)null);

        /// <summary>
        ///     Create custom collation.
        /// </summary>
        /// <typeparam name="T">The type of the state object.</typeparam>
        /// <param name="name">Name of the collation.</param>
        /// <param name="state">State object passed to each invocation of the collation.</param>
        /// <param name="comparison">Method that compares two strings, using additional state.</param>
        public virtual void CreateCollation<T>(string name, T state, Func<T, string, string, int> comparison)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            AcquireHandle();

            var collation = comparison != null ? (v, s1, s2) => comparison((T)v, s1, s2) : (strdelegate_collation)null;

            if (State == ConnectionState.Open)
            {
                var rc = sqlite3_create_collation(_handle._db, name, state, collation);
                SqliteException.ThrowExceptionForRC(rc, _handle._db);
            }

            _collations[name] = (state, collation);
        }

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <returns>The transaction.</returns>
        public new virtual SqliteTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.Unspecified);

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction.</returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => BeginTransaction(isolationLevel);

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction.</returns>
        public new virtual SqliteTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.CallRequiresOpenConnection(nameof(BeginTransaction)));
            }

            if (Transaction != null)
            {
                throw new InvalidOperationException(Resources.ParallelTransactionsNotSupported);
            }

            return Transaction = new SqliteTransaction(this, isolationLevel);
        }

        /// <summary>
        ///     Changes the current database. Not supported.
        /// </summary>
        /// <param name="databaseName">The name of the database to use.</param>
        /// <exception cref="NotSupportedException">Always.</exception>
        public override void ChangeDatabase(string databaseName)
            => throw new NotSupportedException();

        /// <summary>
        ///     Enables extension loading on the connection.
        /// </summary>
        /// <param name="enable">true to enable; false to disable.</param>
        /// <seealso href="http://sqlite.org/loadext.html">Run-Time Loadable Extensions</seealso>
        public virtual void EnableExtensions(bool enable = true)
        {
            if (_state == ConnectionState.Open)
            {
                var rc = sqlite3_enable_load_extension(_handle._db, enable ? 1 : 0);
                SqliteException.ThrowExceptionForRC(rc, _handle._db);

                _handle._extensionsEnabled = enable;
            }

            _extensionsEnabled = enable;
        }

        /// <summary>
        ///     Loads a SQLite extension library.
        /// </summary>
        /// <param name="file">The shared library containing the extension.</param>
        /// <param name="proc">The entry point. If null, the default entry point is used.</param>
        public void LoadExtension(string file, string proc = null)
        {
            if (_state == ConnectionState.Open)
            {
                int rc;

                var extensionsEnabledForLoad = false;
                if (!_extensionsEnabled)
                {
                    rc = sqlite3_enable_load_extension(_handle._db, 1);
                    SqliteException.ThrowExceptionForRC(rc, _handle._db);
                    extensionsEnabledForLoad = true;
                }

                _handle.LoadExtensionCore(file, proc);

                if (extensionsEnabledForLoad)
                {
                    rc = sqlite3_enable_load_extension(_handle._db, 0);
                    SqliteException.ThrowExceptionForRC(rc, _handle._db);
                }
            }

            _extensions.Add((file, proc));
        }


        /// <summary>
        ///     Backup of the connected database.
        /// </summary>
        /// <param name="destination">The destination of the backup.</param>
        public virtual void BackupDatabase(SqliteConnection destination)
            => BackupDatabase(destination, MainDatabaseName, MainDatabaseName);

        /// <summary>
        ///     Backup of the connected database.
        /// </summary>
        /// <param name="destination">The destination of the backup.</param>
        /// <param name="destinationName">The name of the destination database.</param>
        /// <param name="sourceName">The name of the source database.</param>
        public virtual void BackupDatabase(SqliteConnection destination, string destinationName, string sourceName)
        {
            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.CallRequiresOpenConnection(nameof(BackupDatabase)));
            }

            if (destination == null)
            {
                throw new ArgumentNullException(nameof(destination));
            }

            var close = false;
            if (destination.State != ConnectionState.Open)
            {
                destination.Open();
                close = true;
            }

            try
            {
                using (var backup = sqlite3_backup_init(destination._handle._db, destinationName, _handle._db, sourceName))
                {
                    int rc;
                    if (backup.IsInvalid)
                    {
                        rc = sqlite3_errcode(destination._handle._db);
                        SqliteException.ThrowExceptionForRC(rc, destination._handle._db);
                    }

                    rc = sqlite3_backup_step(backup, -1);
                    SqliteException.ThrowExceptionForRC(rc, destination._handle._db);
                }
            }
            finally
            {
                if (close)
                {
                    destination.Close();
                }
            }
        }

        private void CreateFunctionCore<TState, TResult>(
            string name,
            int arity,
            TState state,
            Func<TState, SqliteValueReader, TResult> function,
            bool isDeterministic)
        {
            AcquireHandle();

            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            delegate_function_scalar func = null;
            if (function != null)
            {
                func = (ctx, user_data, args) =>
                {
                    // TODO: Avoid allocation when niladic
                    var values = new SqliteParameterReader(name, args);

                    try
                    {
                        // TODO: Avoid closure by passing function via user_data
                        var result = function((TState)user_data, values);

                        new SqliteResultBinder(ctx, result).Bind();
                    }
                    catch (Exception ex)
                    {
                        sqlite3_result_error(ctx, ex.Message);

                        if (ex is SqliteException sqlEx)
                        {
                            // NB: This must be called after sqlite3_result_error()
                            sqlite3_result_error_code(ctx, sqlEx.SqliteErrorCode);
                        }
                    }
                };
            }

            var flags = isDeterministic ? SQLITE_DETERMINISTIC : 0;

            if (State == ConnectionState.Open)
            {
                var rc = sqlite3_create_function(
                    _handle._db,
                    name,
                    arity,
                    flags,
                    state,
                    func);
                SqliteException.ThrowExceptionForRC(rc, _handle._db);
            }

            _functions[(name, arity)] = (flags, state, func);
        }

        private void CreateAggregateCore<TAccumulate, TResult>(
            string name,
            int arity,
            TAccumulate seed,
            Func<TAccumulate, SqliteValueReader, TAccumulate> func,
            Func<TAccumulate, TResult> resultSelector,
            bool isDeterministic)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            delegate_function_aggregate_step func_step = null;
            if (func != null)
            {
                func_step = (ctx, user_data, args) =>
                {
                    var context = (AggregateContext<TAccumulate>)user_data;
                    if (context.Exception != null)
                    {
                        return;
                    }

                    // TODO: Avoid allocation when niladic
                    var reader = new SqliteParameterReader(name, args);

                    try
                    {
                        // TODO: Avoid closure by passing func via user_data
                        // NB: No need to set ctx.state since we just mutate the instance
                        context.Accumulate = func(context.Accumulate, reader);
                    }
                    catch (Exception ex)
                    {
                        context.Exception = ex;
                    }
                };
            }

            delegate_function_aggregate_final func_final = null;
            if (resultSelector != null)
            {
                func_final = (ctx, user_data) =>
                {
                    var context = (AggregateContext<TAccumulate>)user_data;

                    if (context.Exception == null)
                    {
                        try
                        {
                            // TODO: Avoid closure by passing resultSelector via user_data
                            var result = resultSelector(context.Accumulate);

                            new SqliteResultBinder(ctx, result).Bind();
                        }
                        catch (Exception ex)
                        {
                            context.Exception = ex;
                        }
                    }

                    if (context.Exception != null)
                    {
                        sqlite3_result_error(ctx, context.Exception.Message);

                        if (context.Exception is SqliteException sqlEx)
                        {
                            // NB: This must be called after sqlite3_result_error()
                            sqlite3_result_error_code(ctx, sqlEx.SqliteErrorCode);
                        }
                    }
                };
            }

            var flags = isDeterministic ? SQLITE_DETERMINISTIC : 0;
            var state = new AggregateContext<TAccumulate>(seed);

            if (State == ConnectionState.Open)
            {
                var rc = sqlite3_create_function(
                    _handle._db,
                    name,
                    arity,
                    flags,
                    state,
                    func_step,
                    func_final);
                SqliteException.ThrowExceptionForRC(rc, _handle._db);
            }

            _aggregates[(name, arity)] = (flags, state, func_step, func_final);
        }

        private static Func<TState, SqliteValueReader, TResult> IfNotNull<TState, TResult>(
            object x,
            Func<TState, SqliteValueReader, TResult> value)
            => x != null ? value : null;

        private static object[] GetValues(SqliteValueReader reader)
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);

            return values;
        }

        private class AggregateContext<T>
        {
            public AggregateContext(T seed)
                => Accumulate = seed;

            public T Accumulate { get; set; }
            public Exception Exception { get; set; }
        }

        private class FunctionsKeyComparer : IEqualityComparer<(string name, int arity)>
        {
            public static readonly FunctionsKeyComparer Instance = new FunctionsKeyComparer();

            public bool Equals((string name, int arity) x, (string name, int arity) y)
                => StringComparer.OrdinalIgnoreCase.Equals(x.name, y.name)
                    && x.arity == y.arity;

            public int GetHashCode((string name, int arity) obj)
            {
                var nameHashCode = StringComparer.OrdinalIgnoreCase.GetHashCode(obj.name);
                var arityHashCode = obj.arity.GetHashCode();

                return ((int)(((uint)nameHashCode << 5) | ((uint)nameHashCode >> 27)) + nameHashCode) ^ arityHashCode;
            }
        }
    }
}
