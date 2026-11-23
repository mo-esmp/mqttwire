using GroupChat.Server;
using MqttWire.Server;

// The chat server: MqttWire's connection server with ChatHandler at /mqtt, and the Blazor WebAssembly client of
// GroupChat.Client served from the same origin, so that the client finds the endpoint from its own address.
WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddMqttWireServer<ChatHandler>();

WebApplication app = builder.Build();

// UseWebSockets must come before the MQTT endpoint.
app.UseWebSockets();
app.UseBlazorFrameworkFiles();
app.UseStaticFiles();
app.MapMqttWire("/mqtt");
app.MapFallbackToFile("index.html");
await app.RunAsync();
