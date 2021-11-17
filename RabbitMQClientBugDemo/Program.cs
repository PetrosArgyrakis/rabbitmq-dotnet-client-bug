using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;


namespace RabbitMQTestClient
{
    class Program
    {
        private static string Queue { get; }= "TestQueue";
        
        static async Task Main(string[] args)
        {
            Console.WriteLine("1. Starting\n");
            await Consume();
        }
        
        private static async Task Consume()
        {
            var consumerConnectionFactory = new ConnectionFactory
            {
                ClientProvidedName = "RabbitMQTestConsumer",
                HostName = "localhost",
                Port = AmqpTcpEndpoint.UseDefaultPort,
                VirtualHost =ConnectionFactory.DefaultVHost,
                UserName = ConnectionFactory.DefaultUser,
                Password = ConnectionFactory.DefaultPass,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                DispatchConsumersAsync = true,
                ConsumerDispatchConcurrency = 20
            };
            
            var connection = (IAutorecoveringConnection)consumerConnectionFactory.CreateConnection();
           
            connection.ConnectionShutdown += OnConnectionShutdown;
            connection.RecoverySucceeded += OnRecoverySucceeded;
            connection.ConnectionRecoveryError += OnConnectionRecoveryError;
            
            while (true)
            {
                Console.WriteLine("2. Create Model");
                var model = connection.CreateModel();
                Console.WriteLine("3. Start Consume");
                await ChannelConsumeAsync(model, Queue, CancellationToken.None);
                await Task.Delay(1000);
            }
        }

        private static void OnConnectionShutdown(object sender, ShutdownEventArgs args)
        {
            Console.WriteLine($"OnConnectionShutdown. Reason: {args}");
        }

        private static  void OnRecoverySucceeded(object sender, EventArgs args)
        {
            Console.WriteLine($"OnRecoverySucceeded. Reason: {args.ToString()}");
        }

        private static  void OnConnectionRecoveryError(object sender, ConnectionRecoveryErrorEventArgs args)
        {
            Console.WriteLine($"OnConnectionRecoveryError. Reason: {args}");
        }
       
        private static async Task ChannelConsumeAsync(IModel model, string queueName, CancellationToken cancellationToken)
        {
            var channelTaskCompletionSource = new TaskCompletionSource<bool?>();
            
            async Task OnRegistered(object sender, ConsumerEventArgs args)
            {
                Console.WriteLine($"{DateTimeOffset.UtcNow} OnRegistered. Consumer Tags: {String.Join(",", args.ConsumerTags)} registered");
            }

            async Task OnUnRegistered(object sender, ConsumerEventArgs args)
            {
                Console.WriteLine($"OnUnRegistered. Consumer Tags: {String.Join(",", args.ConsumerTags)}");
            }
            
            async Task OnShutDown(object sender, ShutdownEventArgs args)
            {
                Console.WriteLine($"OnShutDown: {args}");
                channelTaskCompletionSource.SetCanceled();            }
                        
            async Task OnReceived(object sender, BasicDeliverEventArgs args)
            {
                Console.WriteLine($"{DateTimeOffset.UtcNow} OnReceived. Delivery Tag: {args.DeliveryTag}");
                
                try
                {
                    model.BasicAck(args.DeliveryTag, false);
                    model.BasicAck(args.DeliveryTag, false);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
            
            var asyncEventingBasicConsumer = new AsyncEventingBasicConsumer(model);
           
            asyncEventingBasicConsumer.Shutdown += OnShutDown;
            asyncEventingBasicConsumer.Received += OnReceived;
            asyncEventingBasicConsumer.Registered += OnRegistered;
            asyncEventingBasicConsumer.Unregistered += OnUnRegistered;
            
            var consumerTag = $"RabbitMQTestClient_{Guid.NewGuid()}";
            
            cancellationToken.Register(() =>
            {
                Console.WriteLine($"{DateTimeOffset.UtcNow} Cancelling consumer with tag: '{consumerTag}'");
                channelTaskCompletionSource.SetCanceled();
            });
            
            try
            {
                model.BasicQos(0,
                    20,
                    false);

                model.BasicConsume(
                    queueName,
                    false,
                    consumerTag,
                    false,
                    false,
                    new Dictionary<string, object>(),
                    asyncEventingBasicConsumer);

                Console.WriteLine($"{DateTimeOffset.UtcNow} Consuming with {consumerTag}");
                
                await channelTaskCompletionSource.Task;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                asyncEventingBasicConsumer.Shutdown -= OnShutDown;
                asyncEventingBasicConsumer.Received -= OnReceived;
                asyncEventingBasicConsumer.Registered -= OnRegistered;
                asyncEventingBasicConsumer.Unregistered -= OnUnRegistered;
            }
        }
    }
}