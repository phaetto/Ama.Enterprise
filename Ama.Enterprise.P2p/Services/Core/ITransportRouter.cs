namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Routes outgoing messages to the appropriate transport implementation based on the endpoint type.
/// </summary>
/// <typeparam name="TMessage">The type of the message being transported.</typeparam>
public interface ITransportRouter<in TMessage> : ITransport<TMessage>
{
}