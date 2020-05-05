// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public class TemporalQueryRootExpression : QueryRootExpression
    {
        public TemporalQueryRootExpression(
            [NotNull] IAsyncQueryProvider queryProvider, [NotNull] IEntityType entityType, [NotNull] DateTime pointInTime)
            : base(queryProvider, entityType)
        {
            Check.NotNull(pointInTime, nameof(pointInTime));

            PointInTime = pointInTime;
        }

        public TemporalQueryRootExpression(
            [NotNull] IEntityType entityType, [NotNull] DateTime pointInTime)
            : base(entityType)
        {
            Check.NotNull(pointInTime, nameof(pointInTime));

            PointInTime = pointInTime;
        }

        public virtual DateTime PointInTime { get; }

        public override Expression DetachQueryProvider() => new TemporalQueryRootExpression(EntityType, PointInTime);

        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => this;

        public override void Print(ExpressionPrinter expressionPrinter)
        {
            base.Print(expressionPrinter);
            expressionPrinter.Append($".AsOf({PointInTime})");
        }

        public override bool Equals(object obj)
            => obj != null
                && (ReferenceEquals(this, obj)
                    || obj is TemporalQueryRootExpression queryRootExpression
                    && Equals(queryRootExpression));

        private bool Equals(TemporalQueryRootExpression queryRootExpression)
            => base.Equals(queryRootExpression)
                && PointInTime == queryRootExpression.PointInTime;

        public override int GetHashCode()
            => HashCode.Combine(base.GetHashCode(), PointInTime);
    }
}
