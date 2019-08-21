// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using Microsoft.Data.Sqlite.Properties;
using SQLitePCL;

using static SQLitePCL.raw;

namespace Microsoft.Data.Sqlite
{
    /// <summary>
    ///     Represents a connection to a SQLite database.
    /// </summary>
    public class SqliteConnectionHandle : IDisposable
    {
        internal const string MainDatabaseName = "main";

        private const string DataDirectoryMacro = "|DataDirectory|";

        private List<WeakReference<SqliteCommand>> _commands;

        private Dictionary<string, (object state, strdelegate_collation collation)> _collations;

        private Dictionary<(string name, int arity), (int flags, object state, delegate_function_scalar func)> _functions;

        private Dictionary<(string name, int arity), (int flags, object state, delegate_function_aggregate_step func_step, delegate_function_aggregate_final func_final)> _aggregates;

        private HashSet<(string file, string proc)> _extensions;

        internal SqliteConnection _wrapper;
        internal string _connectionString;
        internal ConnectionState _state;
        internal sqlite3 _db;
        internal bool _extensionsEnabled;
        internal int _defaultTimeout;

        /// <summary>
        ///     Initializes a new instance of the <see cref="SqliteConnectionHandle" /> class.
        /// </summary>
        internal SqliteConnectionHandle()
        {
        }

        internal void Initialize(SqliteConnection wrapper)
        {
            _commands = wrapper._commands;
            _functions = wrapper._functions;
            _aggregates = wrapper._aggregates;
            _collations = wrapper._collations;
            _extensions = wrapper._extensions;
            _extensionsEnabled = wrapper._extensionsEnabled;

            _wrapper = wrapper;
            _connectionString = null;
            _defaultTimeout = wrapper.DefaultTimeout;
        }

        /// <summary>
        ///     Gets a handle to underlying database connection.
        /// </summary>
        /// <value>A handle to underlying database connection.</value>
        /// <seealso href="http://sqlite.org/c3ref/sqlite3.html">Database Connection Handle</seealso>
        public virtual sqlite3 Handle
            => _db;
        
        internal SqliteConnectionStringBuilder ConnectionOptions { get; set; }
        
        /// <summary>
        ///     Gets or sets the transaction currently being used by the connection, or null if none.
        /// </summary>
        /// <value>The transaction currently being used by the connection.</value>
        protected internal virtual SqliteTransaction Transaction { get; set; }

        internal void Open()
        {
            if (_state == ConnectionState.Open)
            {
                return;
            }

            if (_connectionString == null)
            {
                throw new InvalidOperationException(Resources.OpenRequiresSetConnectionString);
            }

            var filename = ConnectionOptions.DataSource;
            var flags = 0;

            if (filename.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
            {
                flags |= SQLITE_OPEN_URI;
            }

            switch (ConnectionOptions.Mode)
            {
                case SqliteOpenMode.ReadOnly:
                    flags |= SQLITE_OPEN_READONLY;
                    break;

                case SqliteOpenMode.ReadWrite:
                    flags |= SQLITE_OPEN_READWRITE;
                    break;

                case SqliteOpenMode.Memory:
                    flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY;
                    if ((flags & SQLITE_OPEN_URI) == 0)
                    {
                        flags |= SQLITE_OPEN_URI;
                        filename = "file:" + filename;
                    }

                    break;

                default:
                    Debug.Assert(
                        ConnectionOptions.Mode == SqliteOpenMode.ReadWriteCreate,
                        "ConnectionOptions.Mode is not ReadWriteCreate");
                    flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
                    break;
            }

            switch (ConnectionOptions.Cache)
            {
                case SqliteCacheMode.Shared:
                    flags |= SQLITE_OPEN_SHAREDCACHE;
                    break;

                case SqliteCacheMode.Private:
                    flags |= SQLITE_OPEN_PRIVATECACHE;
                    break;

                default:
                    Debug.Assert(
                        ConnectionOptions.Cache == SqliteCacheMode.Default,
                        "ConnectionOptions.Cache is not Default.");
                    break;
            }

            var dataDirectory = AppDomain.CurrentDomain.GetData("DataDirectory") as string;
            if (!string.IsNullOrEmpty(dataDirectory)
                && (flags & SQLITE_OPEN_URI) == 0
                && !filename.Equals(":memory:", StringComparison.OrdinalIgnoreCase))
            {
                if (filename.StartsWith(DataDirectoryMacro, StringComparison.InvariantCultureIgnoreCase))
                {
                    filename = Path.Combine(dataDirectory, filename.Substring(DataDirectoryMacro.Length));
                }
                else if (!Path.IsPathRooted(filename))
                {
                    filename = Path.Combine(dataDirectory, filename);
                }
            }

            var rc = sqlite3_open_v2(filename, out _db, flags, vfs: null);
            SqliteException.ThrowExceptionForRC(rc, _db);

            _state = ConnectionState.Open;
            try
            {
                if (!string.IsNullOrEmpty(ConnectionOptions.Password))
                {
                    if (SQLitePCLExtensions.EncryptionNotSupported())
                    {
                        throw new InvalidOperationException(Resources.EncryptionNotSupported);
                    }

                    // NB: SQLite doesn't support parameters in PRAGMA statements, so we escape the value using the
                    //     quote function before concatenating.
                    var quotedPassword = _wrapper.ExecuteScalar<string>(
                        "SELECT quote($password);",
                        new SqliteParameter("$password", ConnectionOptions.Password));
                    _wrapper.ExecuteNonQuery("PRAGMA key = " + quotedPassword + ";");

                    // NB: Forces decryption. Throws when the key is incorrect.
                    _wrapper.ExecuteNonQuery("SELECT COUNT(*) FROM sqlite_master;");
                }

                if (ConnectionOptions.ForeignKeys.HasValue)
                {
                    _wrapper.ExecuteNonQuery(
                        "PRAGMA foreign_keys = " + (ConnectionOptions.ForeignKeys.Value ? "1" : "0") + ";");
                }

                if (ConnectionOptions.RecursiveTriggers)
                {
                    _wrapper.ExecuteNonQuery("PRAGMA recursive_triggers = 1;");
                }

                foreach (var item in _collations)
                {
                    rc = sqlite3_create_collation(_db, item.Key, item.Value.state, item.Value.collation);
                    SqliteException.ThrowExceptionForRC(rc, _db);
                }

                foreach (var item in _functions)
                {
                    rc = sqlite3_create_function(_db, item.Key.name, item.Key.arity, item.Value.state, item.Value.func);
                    SqliteException.ThrowExceptionForRC(rc, _db);
                }

                foreach (var item in _aggregates)
                {
                    rc = sqlite3_create_function(_db, item.Key.name, item.Key.arity, item.Value.state, item.Value.func_step, item.Value.func_final);
                    SqliteException.ThrowExceptionForRC(rc, _db);
                }

                var extensionsEnabledForLoad = false;
                if (_extensions.Count != 0)
                {
                    rc = sqlite3_enable_load_extension(_db, 1);
                    SqliteException.ThrowExceptionForRC(rc, _db);
                    extensionsEnabledForLoad = true;

                    foreach (var item in _extensions)
                    {
                        LoadExtensionCore(item.file, item.proc);
                    }
                }

                if (_extensionsEnabled != extensionsEnabledForLoad)
                {
                    rc = sqlite3_enable_load_extension(_db, _extensionsEnabled ? 1 : 0);
                    SqliteException.ThrowExceptionForRC(rc, _db);
                }
            }
            catch
            {
                _db.Dispose();
                _db = null;

                _state = ConnectionState.Closed;

                throw;
            }
        }

        internal void Close()
        {
            if (_db != null)
            {
                _db.Dispose();
                _db = null;
            }

            _state = ConnectionState.Closed;
        }

        internal void Release()
        {
            _wrapper = null;

            if (_state != ConnectionState.Open)
            {
                return;
            }

            Transaction?.Dispose();

            // command.Dispose() removes itself from _commands
            for (var i = _commands.Count - 1; i >= 0; i--)
            {
                var reference = _commands[i];
                if (reference.TryGetTarget(out var command))
                {
                    command.Dispose();
                }
            }

            Debug.Assert(_commands.Count == 0);

            // These collections are references to the wrapper's ones
            _commands = null;
            _functions = null;
            _aggregates = null;
            _collations = null;
            _extensions = null;
        }

        internal void LoadExtensionCore(string file, string proc)
        {
            if (proc == null)
            {
                // NB: SQLitePCL.raw doesn't expose sqlite3_load_extension()
                _wrapper.ExecuteNonQuery(
                    "SELECT load_extension($file);",
                    new SqliteParameter("$file", file));
            }
            else
            {
                _wrapper.ExecuteNonQuery(
                    "SELECT load_extension($file, $proc);",
                    new SqliteParameter("$file", file),
                    new SqliteParameter("$proc", proc));
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

            var collation = comparison != null ? (v, s1, s2) => comparison((T)v, s1, s2) : (strdelegate_collation)null;

            if (_state == ConnectionState.Open)
            {
                var rc = sqlite3_create_collation(_db, name, state, collation);
                SqliteException.ThrowExceptionForRC(rc, _db);
            }

            _collations[name] = (state, collation);
        }

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <returns>The transaction.</returns>
        public SqliteTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.Unspecified);

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction.</returns>
        protected DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => BeginTransaction(isolationLevel);

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction.</returns>
        public SqliteTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            if (_state != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.CallRequiresOpenConnection(nameof(BeginTransaction)));
            }

            if (Transaction != null)
            {
                throw new InvalidOperationException(Resources.ParallelTransactionsNotSupported);
            }

            return Transaction = new SqliteTransaction(_wrapper, isolationLevel);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Close();
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
