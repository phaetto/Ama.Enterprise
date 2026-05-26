namespace Ama.Enterprise.CRDT.MessagePack.Extensions;

using Ama.CRDT.Models;
using Ama.CRDT.Models.Partitioning;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.MessagePack.Formatters;
using Ama.Enterprise.Licensing.Extensions;
using global::MessagePack;
using global::MessagePack.Formatters;
using global::MessagePack.Resolvers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

/// <summary>
/// Extension methods for configuring MessagePack binary serialization for the CRDT engine.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Replaces the default System.Text.Json CRDT serializer with a highly optimized MessagePack binary serializer.
    /// This method registers the internal base resolvers and hooks up the polymorphic formatters.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="customResolvers">Optional source-generated resolvers (e.g. from multi-project context bounds) containing formatters for your types.</param>
    public static IServiceCollection AddCrdtMessagePack(this IServiceCollection services, params IFormatterResolver[] customResolvers)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddAmaEnterpriseLicense();

        services.TryAddKeyedSingleton("Ama.CRDT.MessagePack", (sp, key) =>
        {
            var formatters = new IMessagePackFormatter[]
            {
                // Bridging dynamically mapped string polymorphism cleanly over to MessagePack binary format
                new CrdtPolymorphicMessagePackFormatter<object>(),
                new CrdtPolymorphicMessagePackFormatter<IComparable>(),
                new CrdtPolymorphicMessagePackFormatter<ICrdtTimestamp>(),
                new CrdtPolymorphicMessagePackFormatter<IPartition>(),
                new CrdtPolymorphicMessagePackFormatter<ICrdtMetadataState>()
            };

            var resolvers = new IFormatterResolver[(customResolvers?.Length ?? 0) + 1];
            
            if (customResolvers != null)
            {
                for (int i = 0; i < customResolvers.Length; i++)
                {
                    resolvers[i] = customResolvers[i];
                }
            }
            
            resolvers[^1] = StandardResolver.Instance;

            var resolver = CompositeResolver.Create(formatters, resolvers);

            return MessagePackSerializerOptions.Standard.WithResolver(resolver);
        });

        // Swap out the STJ serializer for our binary implementation
        var descriptor = ServiceDescriptor.Singleton<ICrdtSerializer, MessagePackCrdtSerializer>();
        services.Replace(descriptor);

        return services;
    }
}