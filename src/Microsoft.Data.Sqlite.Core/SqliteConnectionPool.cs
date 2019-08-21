// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;

namespace Microsoft.Data.Sqlite
{
    /// <summary>
    /// Summary.
    /// </summary>
    public class SqliteConnectionPool : IDisposable
    {
        private readonly SqliteConnectionHandle[] _handles;

        /// <summary>
        ///     Initializes a new instance of the <see cref="SqliteConnectionPool" /> class.
        /// </summary>
        /// <param name="maximumRetained">maximumRetained.</param>
        public SqliteConnectionPool(int maximumRetained)
        {
            _handles = new SqliteConnectionHandle[maximumRetained];
        }

        /// <summary>
        /// Rents.
        /// </summary>
        /// <returns>Something.</returns>
        public SqliteConnectionHandle Rent()
        {
            for (var i = 0; i < _handles.Length; i++)
            {
                var item = _handles[i];

                if (item != null && Interlocked.CompareExchange(ref _handles[i], null, item) == item)
                {
                    return item;
                }
            }

            return new SqliteConnectionHandle();
        }

        /// <summary>
        /// Returns.
        /// </summary>
        /// <param name="session">session.</param>
        public void Return(SqliteConnectionHandle session)
        {
            for (var i = 0; i < _handles.Length; i++)
            {
                if (Interlocked.CompareExchange(ref _handles[i], session, null) == null)
                {
                    return;
                }
            }

            session.Dispose();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            for (var i = 0; i < _handles.Length; i++)
            {
                _handles[i]?.Dispose();
            }
        }
    }
}
