namespace Ama.Enterprise.CRDT.MessagePack.Analyzers.UnitTests;

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json.Serialization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Shouldly;
using Xunit;

public sealed class MessagePackFormatterGeneratorTests
{
    private const string MessagePackStubs = @"
// Stubs to allow the generated test code to compile successfully without bringing in full MessagePack dependencies
namespace MessagePack
{
    public class MessagePackFormatterAttribute : System.Attribute {
        public MessagePackFormatterAttribute(System.Type formatterType) {}
    }

    public interface IFormatterResolver { IMessagePackFormatter<T> GetFormatterWithVerify<T>(); }
    public class MessagePackSerializerOptions { public IFormatterResolver Resolver { get; } }
    public struct MessagePackWriter { public void WriteNil() {} public void WriteArrayHeader(int count) {} }
    public struct MessagePackReader { public bool TryReadNil() => false; public int ReadArrayHeader() => 0; public void Skip() {} }
    
    namespace Formatters
    {
        public interface IMessagePackFormatter<T> { 
            void Serialize(ref MessagePackWriter w, T v, MessagePackSerializerOptions o);
            T Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o);
        }
        public class ListFormatter<T> : IMessagePackFormatter<System.Collections.Generic.List<T>> { 
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.List<T> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.List<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class DictionaryFormatter<K,V> : IMessagePackFormatter<System.Collections.Generic.Dictionary<K,V>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.Dictionary<K,V> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.Dictionary<K,V> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class NullableFormatter<T> : IMessagePackFormatter<System.Nullable<T>> where T : struct {
            public void Serialize(ref MessagePackWriter w, System.Nullable<T> v, MessagePackSerializerOptions o) {} 
            public System.Nullable<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => default;
        }
        public class ArrayFormatter<T> : IMessagePackFormatter<T[]> {
            public void Serialize(ref MessagePackWriter w, T[] v, MessagePackSerializerOptions o) {} 
            public T[] Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class HashSetFormatter<T> : IMessagePackFormatter<System.Collections.Generic.HashSet<T>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.HashSet<T> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.HashSet<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class InterfaceListFormatter<T> : IMessagePackFormatter<System.Collections.Generic.IList<T>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.IList<T> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.IList<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class InterfaceReadOnlyListFormatter<T> : IMessagePackFormatter<System.Collections.Generic.IReadOnlyList<T>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.IReadOnlyList<T> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.IReadOnlyList<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class InterfaceEnumerableFormatter<T> : IMessagePackFormatter<System.Collections.Generic.IEnumerable<T>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.IEnumerable<T> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.IEnumerable<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class InterfaceSetFormatter<T> : IMessagePackFormatter<System.Collections.Generic.ISet<T>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.ISet<T> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.ISet<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class InterfaceReadOnlySetFormatter<T> : IMessagePackFormatter<System.Collections.Generic.IReadOnlySet<T>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.IReadOnlySet<T> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.IReadOnlySet<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class InterfaceDictionaryFormatter<K,V> : IMessagePackFormatter<System.Collections.Generic.IDictionary<K,V>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.IDictionary<K,V> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.IDictionary<K,V> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
        public class InterfaceReadOnlyDictionaryFormatter<K,V> : IMessagePackFormatter<System.Collections.Generic.IReadOnlyDictionary<K,V>> {
            public void Serialize(ref MessagePackWriter w, System.Collections.Generic.IReadOnlyDictionary<K,V> v, MessagePackSerializerOptions o) {} 
            public System.Collections.Generic.IReadOnlyDictionary<K,V> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
        }
    }
}
namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public static class CrdtPolymorphicMessagePackRegistry
    {
        public static void Register<T>() {}
    }
    
    public sealed class CrdtPolymorphicMessagePackFormatter<T> : global::MessagePack.Formatters.IMessagePackFormatter<T>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter w, T v, global::MessagePack.MessagePackSerializerOptions o) {} 
        public T Deserialize(ref global::MessagePack.MessagePackReader r, global::MessagePack.MessagePackSerializerOptions o) => default;
    }
}
namespace System.Runtime.CompilerServices
{
    public sealed class ModuleInitializerAttribute : System.Attribute {}
}
";

    [Fact]
    public void WhenJsonSerializableIsApplied_ShouldGenerateFormattersAndTraverseProperties()
    {
        var source = @"
using System.Text.Json.Serialization;
using System.Collections.Generic;

namespace TestNamespace
{
    public record MyModel(string Name, List<int> Values, Dictionary<string, MyChildModel> Children);

    public record MyChildModel(int Id);

    [JsonSerializable(typeof(MyModel))]
    public partial class TestJsonContext : JsonSerializerContext
    {
    }
}
" + MessagePackStubs;

        var expectedGeneratedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(global::System.Collections.Generic.Dictionary<string, global::TestNamespace.MyChildModel>)) formatter = new global::MessagePack.Formatters.DictionaryFormatter<string, global::TestNamespace.MyChildModel>();
                if (type == typeof(global::System.Collections.Generic.List<int>)) formatter = new global::MessagePack.Formatters.ListFormatter<int>();
                if (type == typeof(global::TestNamespace.MyChildModel)) formatter = new CustomFormatterWrapper<global::TestNamespace.MyChildModel>(new TestNamespace_MyChildModelFormatter());
                if (type == typeof(global::TestNamespace.MyModel)) formatter = new CustomFormatterWrapper<global::TestNamespace.MyModel>(new TestNamespace_MyModelFormatter());
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::System.Collections.Generic.Dictionary<string, global::TestNamespace.MyChildModel>>();
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::System.Collections.Generic.List<int>>();
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.MyChildModel>();
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.MyModel>();
        }
    }

    public sealed class TestNamespace_MyChildModelFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.MyChildModel>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.MyChildModel value, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (value == null) { writer.WriteNil(); return; }
            writer.WriteArrayHeader(1);
            options.Resolver.GetFormatterWithVerify<int>().Serialize(ref writer, value.Id, options);
        }

        public global::TestNamespace.MyChildModel Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil()) return default;
            var count = reader.ReadArrayHeader();
            int p0 = default;
            if (count > 0) p0 = options.Resolver.GetFormatterWithVerify<int>().Deserialize(ref reader, options);
            for (int i = 1; i < count; i++) reader.Skip();
            return new global::TestNamespace.MyChildModel(p0);
        }
    }

    public sealed class TestNamespace_MyModelFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.MyModel>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.MyModel value, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (value == null) { writer.WriteNil(); return; }
            writer.WriteArrayHeader(3);
            options.Resolver.GetFormatterWithVerify<string>().Serialize(ref writer, value.Name, options);
            options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.List<int>>().Serialize(ref writer, value.Values, options);
            options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.Dictionary<string, global::TestNamespace.MyChildModel>>().Serialize(ref writer, value.Children, options);
        }

        public global::TestNamespace.MyModel Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil()) return default;
            var count = reader.ReadArrayHeader();
            string p0 = default;
            if (count > 0) p0 = options.Resolver.GetFormatterWithVerify<string>().Deserialize(ref reader, options);
            global::System.Collections.Generic.List<int> p1 = default;
            if (count > 1) p1 = options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.List<int>>().Deserialize(ref reader, options);
            global::System.Collections.Generic.Dictionary<string, global::TestNamespace.MyChildModel> p2 = default;
            if (count > 2) p2 = options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.Dictionary<string, global::TestNamespace.MyChildModel>>().Deserialize(ref reader, options);
            for (int i = 3; i < count; i++) reader.Skip();
            return new global::TestNamespace.MyModel(p0, p1, p2);
        }
    }
}
";

        RunGeneratorAndAssertOutput(source, expectedGeneratedCode);
    }

    [Fact]
    public void WhenGenericTypeHasMessagePackFormatterAttribute_ShouldSkipGeneratingFormatterButTraverseArguments()
    {
        var source = @"
using System.Text.Json.Serialization;
using MessagePack;

namespace TestNamespace
{
    [MessagePackFormatter(typeof(CustomFormatter<>))]
    public class CustomGeneric<T> 
    { 
        public T Value { get; set; } 
    }

    public record InnerDto(string Data);

    public class CustomFormatter<T> : global::MessagePack.Formatters.IMessagePackFormatter<CustomGeneric<T>> 
    {
        public void Serialize(ref MessagePackWriter w, CustomGeneric<T> v, MessagePackSerializerOptions o) {} 
        public CustomGeneric<T> Deserialize(ref MessagePackReader r, MessagePackSerializerOptions o) => null;
    }

    [JsonSerializable(typeof(CustomGeneric<InnerDto>))]
    public partial class TestJsonContext : JsonSerializerContext { }
}
" + MessagePackStubs;

        var expectedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(global::TestNamespace.InnerDto)) formatter = new CustomFormatterWrapper<global::TestNamespace.InnerDto>(new TestNamespace_InnerDtoFormatter());
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.InnerDto>();
        }
    }

    public sealed class TestNamespace_InnerDtoFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.InnerDto>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.InnerDto value, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (value == null) { writer.WriteNil(); return; }
            writer.WriteArrayHeader(1);
            options.Resolver.GetFormatterWithVerify<string>().Serialize(ref writer, value.Data, options);
        }

        public global::TestNamespace.InnerDto Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil()) return default;
            var count = reader.ReadArrayHeader();
            string p0 = default;
            if (count > 0) p0 = options.Resolver.GetFormatterWithVerify<string>().Deserialize(ref reader, options);
            for (int i = 1; i < count; i++) reader.Skip();
            return new global::TestNamespace.InnerDto(p0);
        }
    }
}";
        RunGeneratorAndAssertOutput(source, expectedCode);
    }

    [Fact]
    public void WhenTypeIsStruct_ShouldOmitNullCheck()
    {
        var source = @"
using System.Text.Json.Serialization;

namespace TestNamespace
{
    public struct MyStruct
    {
        public int? Id { get; }
        public MyStruct(int? id) { Id = id; }
    }

    [JsonSerializable(typeof(MyStruct))]
    public partial class TestJsonContext : JsonSerializerContext { }
}
" + MessagePackStubs;

        var expectedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(int?)) formatter = new global::MessagePack.Formatters.NullableFormatter<int>();
                if (type == typeof(global::TestNamespace.MyStruct)) formatter = new CustomFormatterWrapper<global::TestNamespace.MyStruct>(new TestNamespace_MyStructFormatter());
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<int?>();
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.MyStruct>();
        }
    }

    public sealed class TestNamespace_MyStructFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.MyStruct>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.MyStruct value, global::MessagePack.MessagePackSerializerOptions options)
        {
            writer.WriteArrayHeader(1);
            options.Resolver.GetFormatterWithVerify<int?>().Serialize(ref writer, value.Id, options);
        }

        public global::TestNamespace.MyStruct Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil()) return default;
            var count = reader.ReadArrayHeader();
            int? p0 = default;
            if (count > 0) p0 = options.Resolver.GetFormatterWithVerify<int?>().Deserialize(ref reader, options);
            for (int i = 1; i < count; i++) reader.Skip();
            return new global::TestNamespace.MyStruct(p0);
        }
    }
}";

        RunGeneratorAndAssertOutput(source, expectedCode);
    }

    [Fact]
    public void WhenConstructorHasDefaultValues_ShouldGenerateFallbackAssignments()
    {
        var source = @"
using System.Text.Json.Serialization;

namespace TestNamespace
{
    public record ModelWithDefaults(string Text = ""Fallback"", bool IsActive = true, int Number = 42);

    [JsonSerializable(typeof(ModelWithDefaults))]
    public partial class TestJsonContext : JsonSerializerContext { }
}
" + MessagePackStubs;

        var expectedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(global::TestNamespace.ModelWithDefaults)) formatter = new CustomFormatterWrapper<global::TestNamespace.ModelWithDefaults>(new TestNamespace_ModelWithDefaultsFormatter());
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.ModelWithDefaults>();
        }
    }

    public sealed class TestNamespace_ModelWithDefaultsFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.ModelWithDefaults>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.ModelWithDefaults value, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (value == null) { writer.WriteNil(); return; }
            writer.WriteArrayHeader(3);
            options.Resolver.GetFormatterWithVerify<string>().Serialize(ref writer, value.Text, options);
            options.Resolver.GetFormatterWithVerify<bool>().Serialize(ref writer, value.IsActive, options);
            options.Resolver.GetFormatterWithVerify<int>().Serialize(ref writer, value.Number, options);
        }

        public global::TestNamespace.ModelWithDefaults Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil()) return default;
            var count = reader.ReadArrayHeader();
            string p0 = default;
            if (count <= 0) p0 = ""Fallback"";
            if (count > 0) p0 = options.Resolver.GetFormatterWithVerify<string>().Deserialize(ref reader, options);
            bool p1 = default;
            if (count <= 1) p1 = true;
            if (count > 1) p1 = options.Resolver.GetFormatterWithVerify<bool>().Deserialize(ref reader, options);
            int p2 = default;
            if (count <= 2) p2 = 42;
            if (count > 2) p2 = options.Resolver.GetFormatterWithVerify<int>().Deserialize(ref reader, options);
            for (int i = 3; i < count; i++) reader.Skip();
            return new global::TestNamespace.ModelWithDefaults(p0, p1, p2);
        }
    }
}";

        RunGeneratorAndAssertOutput(source, expectedCode);
    }

    [Fact]
    public void WhenGeneratingForCollectionsAndArrays_ShouldMapCorrectFormatters()
    {
        var source = @"
using System.Text.Json.Serialization;
using System.Collections.Generic;

namespace TestNamespace
{
    public record CollectionsModel(int[] ArrayData, HashSet<string> SetData, IReadOnlyList<long> ListData, ISet<bool> InterfaceSetData);

    [JsonSerializable(typeof(CollectionsModel))]
    public partial class TestJsonContext : JsonSerializerContext { }
}
" + MessagePackStubs;

        var expectedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(global::System.Collections.Generic.HashSet<string>)) formatter = new global::MessagePack.Formatters.HashSetFormatter<string>();
                if (type == typeof(global::System.Collections.Generic.IReadOnlyList<long>)) formatter = new global::MessagePack.Formatters.InterfaceReadOnlyListFormatter<long>();
                if (type == typeof(global::System.Collections.Generic.ISet<bool>)) formatter = new global::MessagePack.Formatters.InterfaceSetFormatter<bool>();
                if (type == typeof(int[])) formatter = new global::MessagePack.Formatters.ArrayFormatter<int>();
                if (type == typeof(global::TestNamespace.CollectionsModel)) formatter = new CustomFormatterWrapper<global::TestNamespace.CollectionsModel>(new TestNamespace_CollectionsModelFormatter());
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::System.Collections.Generic.HashSet<string>>();
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::System.Collections.Generic.IReadOnlyList<long>>();
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::System.Collections.Generic.ISet<bool>>();
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<int[]>();
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.CollectionsModel>();
        }
    }

    public sealed class TestNamespace_CollectionsModelFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.CollectionsModel>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.CollectionsModel value, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (value == null) { writer.WriteNil(); return; }
            writer.WriteArrayHeader(4);
            options.Resolver.GetFormatterWithVerify<int[]>().Serialize(ref writer, value.ArrayData, options);
            options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.HashSet<string>>().Serialize(ref writer, value.SetData, options);
            options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.IReadOnlyList<long>>().Serialize(ref writer, value.ListData, options);
            options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.ISet<bool>>().Serialize(ref writer, value.InterfaceSetData, options);
        }

        public global::TestNamespace.CollectionsModel Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil()) return default;
            var count = reader.ReadArrayHeader();
            int[] p0 = default;
            if (count > 0) p0 = options.Resolver.GetFormatterWithVerify<int[]>().Deserialize(ref reader, options);
            global::System.Collections.Generic.HashSet<string> p1 = default;
            if (count > 1) p1 = options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.HashSet<string>>().Deserialize(ref reader, options);
            global::System.Collections.Generic.IReadOnlyList<long> p2 = default;
            if (count > 2) p2 = options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.IReadOnlyList<long>>().Deserialize(ref reader, options);
            global::System.Collections.Generic.ISet<bool> p3 = default;
            if (count > 3) p3 = options.Resolver.GetFormatterWithVerify<global::System.Collections.Generic.ISet<bool>>().Deserialize(ref reader, options);
            for (int i = 4; i < count; i++) reader.Skip();
            return new global::TestNamespace.CollectionsModel(p0, p1, p2, p3);
        }
    }
}";
        RunGeneratorAndAssertOutput(source, expectedCode);
    }

    [Fact]
    public void WhenDuplicateTypesAreRegistered_ShouldGenerateOnlyOneFormatter()
    {
        var source = @"
using System.Text.Json.Serialization;

namespace TestNamespace
{
    public record DuplicatedModel(string Value);

    [JsonSerializable(typeof(DuplicatedModel))]
    public partial class Context1 : JsonSerializerContext { }

    [JsonSerializable(typeof(DuplicatedModel))]
    public partial class Context2 : JsonSerializerContext { }
}
" + MessagePackStubs;

        var expectedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(global::TestNamespace.DuplicatedModel)) formatter = new CustomFormatterWrapper<global::TestNamespace.DuplicatedModel>(new TestNamespace_DuplicatedModelFormatter());
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.DuplicatedModel>();
        }
    }

    public sealed class TestNamespace_DuplicatedModelFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.DuplicatedModel>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.DuplicatedModel value, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (value == null) { writer.WriteNil(); return; }
            writer.WriteArrayHeader(1);
            options.Resolver.GetFormatterWithVerify<string>().Serialize(ref writer, value.Value, options);
        }

        public global::TestNamespace.DuplicatedModel Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil()) return default;
            var count = reader.ReadArrayHeader();
            string p0 = default;
            if (count > 0) p0 = options.Resolver.GetFormatterWithVerify<string>().Deserialize(ref reader, options);
            for (int i = 1; i < count; i++) reader.Skip();
            return new global::TestNamespace.DuplicatedModel(p0);
        }
    }
}";
        RunGeneratorAndAssertOutput(source, expectedCode);
    }

    [Fact]
    public void WhenTypeHasNoPublicConstructor_ShouldGenerateThrowingFormatter()
    {
        var source = @"
using System.Text.Json.Serialization;

namespace TestNamespace
{
    public class PrivateClass
    {
        private PrivateClass() {}
    }

    [JsonSerializable(typeof(PrivateClass))]
    public partial class TestJsonContext : JsonSerializerContext { }
}
" + MessagePackStubs;

        var expectedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(global::TestNamespace.PrivateClass)) formatter = new CustomFormatterWrapper<global::TestNamespace.PrivateClass>(new TestNamespace_PrivateClassFormatter());
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.PrivateClass>();
        }
    }

    public sealed class TestNamespace_PrivateClassFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.PrivateClass>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.PrivateClass value, global::MessagePack.MessagePackSerializerOptions options)
        {
            throw new global::System.NotSupportedException(""Cannot directly serialize parameterless type global::TestNamespace.PrivateClass missing a public constructor."");
        }

        public global::TestNamespace.PrivateClass Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            throw new global::System.NotSupportedException(""Cannot directly deserialize parameterless type global::TestNamespace.PrivateClass missing a public constructor."");
        }
    }
}";
        RunGeneratorAndAssertOutput(source, expectedCode);
    }

    [Fact]
    public void WhenTypeIsAbstractOrInterface_ShouldRouteToPolymorphicFormatter()
    {
        var source = @"
using System.Text.Json.Serialization;

namespace TestNamespace
{
    public abstract class AbstractBase
    {
        public int Id { get; set; }
    }

    public interface IMyInterface 
    { 
        string Name { get; set; } 
    }

    [JsonSerializable(typeof(AbstractBase))]
    [JsonSerializable(typeof(IMyInterface))]
    public partial class TestJsonContext : JsonSerializerContext { }
}
" + MessagePackStubs;

        var expectedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(global::TestNamespace.AbstractBase)) formatter = new global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackFormatter<global::TestNamespace.AbstractBase>();
                if (type == typeof(global::TestNamespace.IMyInterface)) formatter = new global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackFormatter<global::TestNamespace.IMyInterface>();
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
        }
    }
}";
        RunGeneratorAndAssertOutput(source, expectedCode);
    }

    [Fact]
    public void WhenTypeHasPropertiesAndParameterlessConstructor_ShouldSerializeProperties()
    {
        var source = @"
using System.Text.Json.Serialization;

namespace TestNamespace
{
    public sealed record TreeNode
    {
        public required object Id { get; init; }
        public object Value { get; set; }
        public object ParentId { get; set; }
    }

    [JsonSerializable(typeof(TreeNode))]
    public partial class TestJsonContext : JsonSerializerContext { }
}
" + MessagePackStubs;

        var expectedCode = @"// <auto-generated/>
#pragma warning disable
#pragma warning disable MsgPack009
using System;
using MessagePack;
using MessagePack.Formatters;

namespace Ama.Enterprise.CRDT.MessagePack.Formatters
{
    public sealed class SourceGeneratorTests_MessagePackResolver : global::MessagePack.IFormatterResolver
    {
        public static readonly global::MessagePack.IFormatterResolver Instance = new SourceGeneratorTests_MessagePackResolver();

        private SourceGeneratorTests_MessagePackResolver() {}

        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()
        {
            return FormatterCache<T>.Formatter;
        }

        public interface ICustomFormatter<TArg>
        {
            void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options);
            TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options);
        }

        public sealed class CustomFormatterWrapper<TArg> : global::MessagePack.Formatters.IMessagePackFormatter<TArg>
        {
            private readonly ICustomFormatter<TArg> _inner;
            public CustomFormatterWrapper(ICustomFormatter<TArg> inner) { _inner = inner; }
            public void Serialize(ref global::MessagePack.MessagePackWriter writer, TArg value, global::MessagePack.MessagePackSerializerOptions options) => _inner.Serialize(ref writer, value, options);
            public TArg Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options) => _inner.Deserialize(ref reader, options);
        }

        private static class FormatterCache<T>
        {
            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;
            static FormatterCache()
            {
                object formatter = null;
                var type = typeof(T);
                if (type == typeof(global::TestNamespace.TreeNode)) formatter = new CustomFormatterWrapper<global::TestNamespace.TreeNode>(new TestNamespace_TreeNodeFormatter());
                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;
            }
        }
    }

    public static class SourceGeneratorTests_MessagePackResolverPolymorphicInitializer
    {
        [global::System.Runtime.CompilerServices.ModuleInitializer]
        public static void Initialize()
        {
            global::Ama.Enterprise.CRDT.MessagePack.Formatters.CrdtPolymorphicMessagePackRegistry.Register<global::TestNamespace.TreeNode>();
        }
    }

    public sealed class TestNamespace_TreeNodeFormatter : SourceGeneratorTests_MessagePackResolver.ICustomFormatter<global::TestNamespace.TreeNode>
    {
        public void Serialize(ref global::MessagePack.MessagePackWriter writer, global::TestNamespace.TreeNode value, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (value == null) { writer.WriteNil(); return; }
            writer.WriteArrayHeader(3);
            options.Resolver.GetFormatterWithVerify<object>().Serialize(ref writer, value.Id, options);
            options.Resolver.GetFormatterWithVerify<object>().Serialize(ref writer, value.ParentId, options);
            options.Resolver.GetFormatterWithVerify<object>().Serialize(ref writer, value.Value, options);
        }

        public global::TestNamespace.TreeNode Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil()) return default;
            var count = reader.ReadArrayHeader();
            object p0 = default;
            if (count > 0) p0 = options.Resolver.GetFormatterWithVerify<object>().Deserialize(ref reader, options);
            object p1 = default;
            if (count > 1) p1 = options.Resolver.GetFormatterWithVerify<object>().Deserialize(ref reader, options);
            object p2 = default;
            if (count > 2) p2 = options.Resolver.GetFormatterWithVerify<object>().Deserialize(ref reader, options);
            for (int i = 3; i < count; i++) reader.Skip();
            return new global::TestNamespace.TreeNode() { Id = p0, ParentId = p1, Value = p2 };
        }
    }
}";
        RunGeneratorAndAssertOutput(source, expectedCode);
    }

    private static void RunGeneratorAndAssertOutput(string source, string expectedGeneratedCode)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(source);

        // Explicitly load the JSON serialization attributes just in case
        _ = typeof(JsonSerializableAttribute);

        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrWhiteSpace(a.Location))
            .Select(a => MetadataReference.CreateFromFile(a.Location))
            .Cast<MetadataReference>()
            .ToList();

        // Safety fallback ensuring System.Runtime is loaded for testing context resolutions
        var systemRuntime = Assembly.Load("System.Runtime");
        if (references.All(r => r.Display != systemRuntime.Location))
        {
            references.Add(MetadataReference.CreateFromFile(systemRuntime.Location));
        }

        var compilation = CSharpCompilation.Create("SourceGeneratorTests",
            new[] { syntaxTree },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new Generators.MessagePackFormatterGenerator();
        var driver = CSharpGeneratorDriver.Create(generator);

        driver.RunGeneratorsAndUpdateCompilation(compilation, out var outputCompilation, out var diagnostics);

        diagnostics.ShouldBeEmpty();

        var generatedTrees = outputCompilation.SyntaxTrees.ToList();
        generatedTrees.Count.ShouldBe(2, "Expected the original syntax tree plus one generated syntax tree.");

        var actualGeneratedCode = generatedTrees.Last().ToString();

        // Normalize line endings and trim space for robust cross-OS evaluation comparisons
        var normalizedActual = actualGeneratedCode.Replace("\r\n", "\n").Trim();
        var normalizedExpected = expectedGeneratedCode.Replace("\r\n", "\n").Trim();

        normalizedActual.ShouldBe(normalizedExpected);
    }
}