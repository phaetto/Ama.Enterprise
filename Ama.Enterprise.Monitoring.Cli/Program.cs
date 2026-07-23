namespace Ama.Enterprise.Monitoring.Cli;

using Ama.Enterprise.CRDT.MessagePack.Extensions;
using Ama.Enterprise.CRDT.MessagePack.Resolvers;
using Ama.Enterprise.Monitoring.Cli.Services;
using Ama.Enterprise.Monitoring.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

internal sealed class Program
{
    public static async Task Main(string[] args)
    {
        args ??= Array.Empty<string>();

        var switchMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "-c", "cert" },
            { "--cert", "cert" },
            { "-k", "key" },
            { "--key", "key" },
            { "-s", "stun" },
            { "--stun", "stun" },
            { "-u", "url" },
            { "--url", "url" },
            { "-b", "binary-serialization" },
            { "--binary-serialization", "binary-serialization" },
            { "-h", "help" },
            { "--help", "help" }
        };

        var configuration = new ConfigurationBuilder()
            .AddCommandLine(args, switchMappings)
            .Build();

        if (configuration.GetValue<bool>("help"))
        {
            PrintHelp();
            return;
        }

        var certFile = configuration.GetValue<string>("cert") ?? string.Empty;
        var keyFile = configuration.GetValue<string>("key") ?? string.Empty;
        var stunArg = configuration.GetValue<string>("stun") ?? string.Empty;
        var monitoringUrl = configuration.GetValue<string>("url") ?? string.Empty;
        var binarySerializationOption = configuration.GetValue<bool>("binary-serialization");

        var stunServers = new List<string>();
        if (!string.IsNullOrWhiteSpace(stunArg))
        {
            stunServers.AddRange(stunArg.Split(',', StringSplitOptions.RemoveEmptyEntries));
        }

        byte[]? certBytes = null;
        string? certThumbprint = null;
        if (!string.IsNullOrWhiteSpace(certFile))
        {
            if (File.Exists(certFile))
            {
                using var cert = X509CertificateLoader.LoadCertificateFromFile(certFile);
                certBytes = cert.Export(X509ContentType.Cert);
                certThumbprint = cert.Thumbprint;
            }
            else
            {
                Console.WriteLine($"Warning: Certificate file '{certFile}' not found. Certificate validation will not be active.");
            }
        }

        string? encryptionKey = null;
        if (!string.IsNullOrWhiteSpace(keyFile))
        {
            if (File.Exists(keyFile))
            {
                encryptionKey = File.ReadAllText(keyFile).Trim();
            }
            else
            {
                Console.WriteLine($"Warning: Encryption key file '{keyFile}' not found. Wire encryption will not be active.");
            }
        }

        var hostBuilder = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                services.AddLogging(configure =>
                {
                    // Console logging is disabled to prevent Terminal.Gui layout corruption.
                    // Future logs can be routed to an in-memory sink displayed within the UI tabs.
                    configure.ClearProviders(); 
                });

                services.AddMonitoringClientMesh(options =>
                {
                    options.IceServers = stunServers.ToArray();
                    options.ParentSignalingUrl = monitoringUrl;
                    options.CertificateBytes = certBytes;
                    options.CertificateThumbprint = certThumbprint;
                    options.EncryptionKeyBase64 = encryptionKey;
                    
                    options.UseBinarySerialization = binarySerializationOption;
                    options.ConfigureBinarySerialization = s => s.AddCrdtMessagePack(
                        Ama_Enterprise_CRDT_MessagePack_MessagePackResolver.Instance,
                        Ama_Enterprise_Monitoring_Cli_MessagePackResolver.Instance
                    );
                });

                services.AddSingleton<IUserInterfaceOrchestrator, UserInterfaceOrchestrator>();
            });

        var host = hostBuilder.Build();

        // Start background host completely decoupled from UI
        await host.StartAsync().ConfigureAwait(false);

        var uiOrchestrator = host.Services.GetRequiredService<IUserInterfaceOrchestrator>();
        uiOrchestrator.Run();

        // Graceful shutdown
        await host.StopAsync().ConfigureAwait(false);
    }

    private static void PrintHelp()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  --cert, -c     Path to the certificate file.");
        Console.WriteLine("  --key, -k      Path to the encryption key file.");
        Console.WriteLine("  --stun, -s     Comma-separated list of STUN/TURN servers.");
        Console.WriteLine("  --url, -u      URL to connect for monitoring.");
        Console.WriteLine("  --binary-serialization, -b     Binary Serialization option configuration.");
        Console.WriteLine("  --help, -h     Show this help message.");
    }
}