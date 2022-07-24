// See https://aka.ms/new-console-template for more information
using Meth;  //will change name and reorganize before any commiting or delivery

Console.WriteLine("Hello, World!");


int hashId = 12345;
Dictionary<string, string> test = new Dictionary<string, string>();


//second pass
Console.WriteLine("Creating Improved producer");
var improved = new Meth.WrappedMethProducer("b-1.mattbroker.gfrhzv.c6.kafka.us-east-2.amazonaws.com:9092", "interestingTopic", test); // "WrappedLib");

//var improved = new Meth.WrappedMethProducer("z-2.mattbroker.gfrhzv.c6.kafka.us-east-2.amazonaws.com:2181", "interestingTopic", test); // "WrappedLib");


//string hashString = "0xdeadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff";

byte[] hash = new byte[] { 0xde, 0xad, 0xbe, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x1f, 0xff, 0xff, 0xff, 0xff, 0xff };
byte[] parent = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
byte[] weight = new byte[] { 0x26, 0x74 }; 


//there is probably a better way to deal with byte[] in C#, dont often use byte array as a data type
Dictionary<string, byte[]> updates = new Dictionary<string, byte[]>(){
    { "a/b", new byte[] {0x88} },
    {"q/17", new byte[] {0x12, 0x34} },
    { "q/18", new byte[] {0x56, 0x78 } }
};

var deletes = new List<string>() { "b/c" }; //["b/c"] for single, multiple ["b/c", "t/g"]
Dictionary<string, byte[]> batches = new Dictionary<string, byte[]>() { };// empty for now... need to figure out batches datatype, since it has subbatches

//call add block with the variables from the example documentation
await improved.AddBlock(1337, hash, parent, weight, updates, deletes, batches );//currently params undefined


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
