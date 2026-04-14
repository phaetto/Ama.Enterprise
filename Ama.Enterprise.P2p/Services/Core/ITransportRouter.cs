namespace Ama.Enterprise.P2p.Services.Core;

using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Routes outgoing messages to the appropriate transport implementation natively based effectively dynamically explicit safely seamlessly.
/// </summary>
/// <typeparam name="TMessage">The type of the message being transported.</typeparam>
public interface ITransportRouter<TMessage> : ITransport<TMessage> where TMessage : IMeshMessage
{
}