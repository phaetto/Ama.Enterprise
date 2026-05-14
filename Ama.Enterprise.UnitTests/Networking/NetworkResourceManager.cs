namespace Ama.Enterprise.UnitTests.Networking;

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

/// <summary>
/// A shared resource manager for providing unique network ports during integration testing.
/// </summary>
public sealed class NetworkResourceManager : IDisposable
{
    private static int currentPort;

    static NetworkResourceManager()
    {
        // Initialize with a random starting port to prevent collisions across multiple test assemblies
        // executing in parallel (which run in separate processes and therefore have their own static fields).
        // This bounds the starting port roughly between 10000 and 40000.
        currentPort = 10000 + (Math.Abs(Guid.NewGuid().GetHashCode()) % 30000);
    }

    /// <summary>
    /// Gets the next available unique network port.
    /// </summary>
    /// <returns>A unique port number that is currently not in use.</returns>
    public int GetNextPort()
    {
        while (true)
        {
            var port = Interlocked.Increment(ref currentPort);
            
            // Loop back if we exceed the dynamic port range
            if (port > 65000)
            {
                Interlocked.Exchange(ref currentPort, 10000 + (Math.Abs(Guid.NewGuid().GetHashCode()) % 30000));
                continue;
            }

            if (IsPortAvailable(port))
            {
                return port;
            }
        }
    }

    private static bool IsPortAvailable(int port)
    {
        try
        {
            // Check TCP
            using var tcpListener = new TcpListener(IPAddress.Loopback, port);
            tcpListener.Start();
            tcpListener.Stop();

            // Check UDP
            using var udpClient = new UdpClient();
            udpClient.Client.Bind(new IPEndPoint(IPAddress.Loopback, port));

            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        // Nothing to dispose
    }
}