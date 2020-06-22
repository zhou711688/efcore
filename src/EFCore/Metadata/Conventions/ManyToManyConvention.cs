// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Metadata.Conventions
{
    /// <summary>
    ///     A convention which looks for matching skip navigations and automatically creates
    ///     a many-to-many association entity with suitable foreign keys, sets the two
    ///     matching skip navigations to use those foreign keys and makes them inverses of
    ///     one another.
    /// </summary>
    public class ManyToManyConvention : ISkipNavigationAddedConvention
    {
        /// <summary>
        ///     Creates a new instance of <see cref="ManyToManyConvention" />.
        /// </summary>
        /// <param name="dependencies"> Parameter object containing dependencies for this convention. </param>
        public ManyToManyConvention([NotNull] ProviderConventionSetBuilderDependencies dependencies)
        {
            Dependencies = dependencies;
        }

        /// <summary>
        ///     Parameter object containing service dependencies.
        /// </summary>
        protected virtual ProviderConventionSetBuilderDependencies Dependencies { get; }

        /// <summary>
        ///     Called after a skip navigation is added to an entity type.
        /// </summary>
        /// <param name="skipNavigationBuilder"> The builder for the skip navigation. </param>
        /// <param name="context"> Additional information associated with convention execution. </param>
        public virtual void ProcessSkipNavigationAdded(
            IConventionSkipNavigationBuilder skipNavigationBuilder,
            IConventionContext<IConventionSkipNavigationBuilder> context)
        {
            Check.NotNull(skipNavigationBuilder, "skipNavigationBuilder");
            Check.NotNull(context, "context");

            var skipNavigation = skipNavigationBuilder.Metadata;
            if (skipNavigation.ForeignKey != null
                || skipNavigation.TargetEntityType == skipNavigation.DeclaringEntityType
                || !skipNavigation.IsCollection)
            {
                // do not create an automatic many-to-many association entity type
                // for a self-referencing skip navigation, or for one that
                // is already "in use" (i.e. has its Foreign Key assigned).
                return;
            }

            var matchingSkipNavigation = skipNavigation.TargetEntityType
                .GetSkipNavigations()
                .FirstOrDefault(sn => sn.TargetEntityType == skipNavigation.DeclaringEntityType);

            if (matchingSkipNavigation == null
                || matchingSkipNavigation.ForeignKey != null
                || !matchingSkipNavigation.IsCollection)
            {
                // do not create an automatic many-to-many association entity type if
                // the matching skip navigation is already "in use" (i.e.
                // has its Foreign Key assigned).
                return;
            }

            var model = (Model)skipNavigation.DeclaringEntityType.Model;
            model.Builder.AssociationEntity(
                (SkipNavigation)skipNavigation,
                (SkipNavigation)matchingSkipNavigation,
                ConfigurationSource.Convention);
        }
    }
}
