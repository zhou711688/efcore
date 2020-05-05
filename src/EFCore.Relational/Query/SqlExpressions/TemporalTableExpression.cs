// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query.SqlExpressions
{
    public sealed class TemporalTableExpression : TableExpressionBase
    {
        internal TemporalTableExpression([NotNull] ITableBase table, [NotNull] DateTime pointInTime)
            : base(table.Name.Substring(0, 1).ToLower())
        {
            Check.NotNull(table, nameof(table));
            Check.NotNull(pointInTime, nameof(pointInTime));

            Name = table.Name;
            Schema = table.Schema;
            PointInTime = pointInTime;
        }

        public string Schema { get; }
        public string Name { get; }
        public DateTime PointInTime { get; }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            Check.NotNull(visitor, nameof(visitor));

            return this;
        }

        public override void Print(ExpressionPrinter expressionPrinter)
        {
            Check.NotNull(expressionPrinter, nameof(expressionPrinter));

            if (!string.IsNullOrEmpty(Schema))
            {
                expressionPrinter.Append(Schema).Append(".");
            }

            expressionPrinter
                .Append(Name)
                .Append(" FOR SYSTEM_TIME AS OF ")
                .Append(PointInTime.ToString())
                .Append(" AS ")
                .Append(Alias);
        }

        public override bool Equals(object obj)
            // This should be reference equal only.
            => obj != null && ReferenceEquals(this, obj);

        public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Name, Schema, PointInTime);
    }
}
