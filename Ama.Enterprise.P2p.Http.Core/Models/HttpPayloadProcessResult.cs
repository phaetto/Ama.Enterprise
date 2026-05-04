namespace Ama.Enterprise.P2p.Http.Core.Models;

/// <summary>
/// Represents the outcome of an inbound HTTP P2P payload processing attempt.
/// </summary>
public enum HttpPayloadProcessResult
{
    /// <summary>
    /// The payload was successfully processed and dispatched.
    /// </summary>
    Success = 0,

    /// <summary>
    /// The payload was malformed or failed to deserialize.
    /// </summary>
    BadRequest = 1,

    /// <summary>
    /// The payload's mesh ID did not match the expected target.
    /// </summary>
    Forbidden = 2,

    /// <summary>
    /// The payload's protocol version is incompatible.
    /// </summary>
    UnsupportedVersion = 3,

    /// <summary>
    /// The target mesh ID has no registered active listener.
    /// </summary>
    ServiceUnavailable = 4,

    /// <summary>
    /// An unexpected error occurred during processing.
    /// </summary>
    InternalServerError = 5
}