// See https://aka.ms/new-console-template for more information
using Meth;  //will change name and reorganize before any commiting or delivery

Console.WriteLine("Hello, World!");


int hashId = 12345;
Dictionary<string, string> test = new Dictionary<string, string>();


//second pass
Console.WriteLine("Creating Improved producer");
var improved = new Meth.WrappedMethProducer("b-1.mattbroker.gfrhzv.c6.kafka.us-east-2.amazonaws.com:9092", "interestingTopic", test); // "WrappedLib");

//var improved = new Meth.WrappedMethProducer("z-2.mattbroker.gfrhzv.c6.kafka.us-east-2.amazonaws.com:2181", "interestingTopic", test); // "WrappedLib");

await improved.AddBlock();//currently params undefined
Thread.Sleep(10000);
Console.WriteLine("Flushing");
improved.Flush();
Console.WriteLine("Flush complete");

Console.WriteLine("Press any key to close");
Console.ReadKey();
return;//end program, don't run first pass code

//first pass below --this code wont run currently
//right now an empty dictionary
Console.WriteLine("Creating Walter White");
var WalterWhite = Meth.MethProducer.NewProducer("URL", "none", test, "WalterWhite");

Console.WriteLine("Creating Jesse Pinkman");
var JessePinkman = Meth.MethProducer.NewProducer("URL44", "othertopics", test, "JessePinkman");

//Meth.MethProducer.AddBroker(WalterWhite, "URL2");

Meth.MethProducer.AddBlock(hashId, WalterWhite, "TopicOfTheDay");  //incomplete method AddBlock, need to look more at Go implementation
Meth.MethProducer.AddBlock(hashId, JessePinkman, "DifferentTopic");

//void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);
//void Produce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);
//WalterWhite.Produce() two ways to send a single message with the producer
//WalterWhite.ProduceAsync() two ways to do it async

Console.WriteLine("Flushing");
WalterWhite.Flush();
Console.WriteLine("Flush complete");

Console.WriteLine("Press any key to close");
Console.ReadKey();
