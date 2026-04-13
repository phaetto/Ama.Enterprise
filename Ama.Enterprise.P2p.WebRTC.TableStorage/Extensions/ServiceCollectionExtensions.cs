namespace Ama.Enterprise.P2p.WebRTC.TableStorage.Extensions;

using System;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.WebRTC.TableStorage.Models;
using Ama.Enterprise.P2p.WebRTC.TableStorage.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

/// <summary>
/// Extension methods configuring fully generic Table Storage out-of-band signaling dynamically correctly.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers Table Storage based out-of-band WebRTC signaling for the current mesh natively.
    /// </summary>
    /// <param name="builder">The mesh builder configuration instance pipeline.</param>
    /// <param name="configureOptions">An action specifying connection bounds mapping logic properly.</param>
    /// <returns>The fully hydrated explicitly updated mesh builder.</returns>
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

        // Add singleton Hosted Services inherently tracking explicit Mesh instances completely decoupling execution safely
        builder.Services.AddSingleton<IHostedService>(sp =>
            ActivatorUtilities.CreateInstance<TableStorageSignalingOfferService>(sp, builder.MeshId));

        builder.Services.AddSingleton<IHostedService>(sp =>
            ActivatorUtilities.CreateInstance<TableStorageSignalingAnswerService>(sp, builder.MeshId));

        return builder;
    }
}