namespace Ama.Enterprise.P2p.WebRTC.TableStorage.Extensions;

using System;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.WebRTC.TableStorage.Models;
using Ama.Enterprise.P2p.WebRTC.TableStorage.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

/// <summary>
/// Extension methods configuring Azure Table Storage out-of-band signaling.
/// This implementation allows distributed peers to automatically exchange WebRTC SDP offers and answers via a shared Azure Table,
/// effectively creating a fully connected P2P mesh network where all active nodes discover and connect to each other.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers Table Storage based out-of-band WebRTC signaling for the current mesh.
    /// This background registration will connect the local node to the overarching full P2P cluster.
    /// </summary>
    /// <param name="builder">The mesh builder configuration instance pipeline.</param>
    /// <param name="configureOptions">An action specifying connection bounds mapping logic.</param>
    /// <returns>The hydrated mesh builder.</returns>
    public static IP2pMeshBuilder AddWebRtcTableStorageSignaling(
        this IP2pMeshBuilder builder,
        Action<TableStorageSignalingOptions>? configureOptions = null)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<TableStorageSignalingOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        // Add singleton Hosted Services tracking explicit Mesh instances decoupling execution
        builder.Services.AddSingleton<IHostedService>(sp =>
            ActivatorUtilities.CreateInstance<TableStorageSignalingOfferService>(sp, builder.MeshId));

        builder.Services.AddSingleton<IHostedService>(sp =>
            ActivatorUtilities.CreateInstance<TableStorageSignalingAnswerService>(sp, builder.MeshId));

        return builder;
    }
}