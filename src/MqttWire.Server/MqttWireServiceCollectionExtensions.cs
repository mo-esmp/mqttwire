using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// Registers the MQTT server with dependency injection.
/// </summary>
public static class MqttWireServiceCollectionExtensions
{
    /// <summary>
    /// Registers the MQTT server: <typeparamref name="THandler"/> as the one <see cref="IMqttWireHandler"/>, the
    /// options, one <see cref="MqttWireServer"/>, and the service that starts it with
    /// the host and stops it, closing every connection, when the host stops. Map its endpoint with
    /// <see cref="MqttWireEndpointRouteBuilderExtensions.MapMqttWire"/>.
    /// </summary>
    /// <typeparam name="THandler">
    /// The host's handler, registered as a singleton: created once by dependency injection, when the host starts,
    /// and called for every connection. It must not depend on scoped services. It may take
    /// <see cref="MqttWireServer"/> in its constructor, to look connections up. A constructor that throws makes the
    /// host's start fail with that exception.
    /// </typeparam>
    /// <param name="services">
    /// The service collection.
    /// </param>
    /// <param name="configure">
    /// Changes the default options, or <see langword="null"/> to keep them. An invalid value throws
    /// <see cref="OptionsValidationException"/> when the server is first resolved (in a <c>WebApplication</c>, that is
    /// the call to <see cref="MqttWireEndpointRouteBuilderExtensions.MapMqttWire"/>) or, at the latest, when the host
    /// starts, which then does not start.
    /// </param>
    /// <returns>
    /// <paramref name="services"/>, for chaining.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// The server was already registered on <paramref name="services"/>; nothing changes.
    /// </exception>
    public static IServiceCollection AddMqttWireServer<THandler>(
        this IServiceCollection services, Action<MqttWireServerOptions>? configure = null)
        where THandler : class, IMqttWireHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        foreach (ServiceDescriptor descriptor in services)
        {
            if (descriptor.ServiceType == typeof(RegistrationMarker))
            {
                Type? registered = (descriptor.ImplementationInstance as RegistrationMarker)?.HandlerType;
                throw new InvalidOperationException(
                    $"AddMqttWireServer was already called on this service collection, with the handler {registered}.");
            }
        }

        services.AddSingleton(new RegistrationMarker(typeof(THandler)));
        services.AddSingleton<IMqttWireHandler, THandler>();
        OptionsBuilder<MqttWireServerOptions> options = services.AddOptions<MqttWireServerOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<MqttWireServerOptions>, MqttWireServerOptionsValidator>());
        // The handler is not resolved here but when the server starts, so that it may take the server itself.
        services.AddSingleton(static provider => new MqttWireServer(
            provider,
            provider.GetRequiredService<IOptions<MqttWireServerOptions>>().Value,
            provider.GetRequiredService<ILogger<MqttWireServer>>(),
            TimeProvider.System,
            ArrayPool<byte>.Shared));
        services.AddHostedService<MqttWireServerLifetime>();
        return services;
    }

    /// <summary>
    /// Marks a service collection on which <see cref="AddMqttWireServer{THandler}"/> was called, so that a second
    /// call is detected whatever handler it names. Registered as an instance; never resolved.
    /// </summary>
    /// <param name="handlerType">
    /// The handler the first call registered.
    /// </param>
    private sealed class RegistrationMarker(Type handlerType)
    {
        /// <summary>
        /// Gets the handler the first call registered, named in the error of a second call.
        /// </summary>
        public Type HandlerType { get; } = handlerType;
    }
}
