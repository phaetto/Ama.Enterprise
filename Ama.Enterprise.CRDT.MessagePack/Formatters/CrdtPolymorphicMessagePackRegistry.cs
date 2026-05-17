namespace Ama.Enterprise.CRDT.MessagePack.Formatters;

using System;
using System.Collections.Concurrent;
using global::MessagePack;

/// <summary>
/// A centralized global registry caching explicit statically typed Native AOT serialization delegates bridging 
/// decoupled polymorphic contexts.
/// </summary>
public static class CrdtPolymorphicMessagePackRegistry
{
    /// <summary>
    /// Delegate encapsulating serialization execution mapping directly to explicit typed bounds.
    /// </summary>
    public delegate void SerializeAction(ref MessagePackWriter writer, object value, MessagePackSerializerOptions options);
    
    /// <summary>
    /// Delegate encapsulating deserialization execution extracting underlying targeted bounds.
    /// </summary>
    public delegate object DeserializeFunc(ref MessagePackReader reader, MessagePackSerializerOptions options);

    private static readonly ConcurrentDictionary<Type, SerializeAction> _serializeMap = new();
    private static readonly ConcurrentDictionary<Type, DeserializeFunc> _deserializeMap = new();

    static CrdtPolymorphicMessagePackRegistry()
    {
        // Pre-register basic CLR types inherently supported by standard MessagePack formatters
        // avoiding explicit manual mapping requirements for common primitives when they get boxed (e.g., inside 'object').
        Register<string>();
        Register<bool>();
        Register<byte>();
        Register<sbyte>();
        Register<short>();
        Register<ushort>();
        Register<int>();
        Register<uint>();
        Register<long>();
        Register<ulong>();
        Register<float>();
        Register<double>();
        Register<decimal>();
        Register<char>();
        Register<Guid>();
        Register<DateTime>();
        Register<DateTimeOffset>();
        Register<TimeSpan>();
        Register<byte[]>();
        Register<Uri>();
    }

    /// <summary>
    /// Registers static AOT formatters resolving concrete dynamic types locally.
    /// </summary>
    public static void Register<T>()
    {
        var type = typeof(T);
        
        if (!_serializeMap.ContainsKey(type))
        {
            _serializeMap.TryAdd(type, (ref MessagePackWriter writer, object value, MessagePackSerializerOptions options) => 
            {
                options.Resolver.GetFormatterWithVerify<T>().Serialize(ref writer, (T)value, options);
            });

            _deserializeMap.TryAdd(type, (ref MessagePackReader reader, MessagePackSerializerOptions options) => 
            {
                return options.Resolver.GetFormatterWithVerify<T>().Deserialize(ref reader, options)!;
            });
        }
    }

    /// <summary>
    /// Attempts to extract the underlying serializer matching explicit type mapping.
    /// </summary>
    public static bool TryGetSerializer(Type type, out SerializeAction? action) => _serializeMap.TryGetValue(type, out action);

    /// <summary>
    /// Attempts to extract the underlying deserializer matching explicit type mapping.
    /// </summary>
    public static bool TryGetDeserializer(Type type, out DeserializeFunc? func) => _deserializeMap.TryGetValue(type, out func);
}