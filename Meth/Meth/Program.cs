
using Meth;  //only thing you need to pull in to use lib,
             //or you could have the project itself be a dependancy in VS
//show project output directory
Console.WriteLine("Hello, World!");


int hashId = 12345; //remove
Dictionary<string, string> test = new Dictionary<string, string>(); //schema map param
test.Add(@"a", "TopicA");
test.Add(@"b", "TopicB"); //i don't know if the / postfix after the key is technically necessary for C#,
test.Add(@"q", "TopicQ"); // for now removing the / postfix
//"a/b/q/" -- inside schema map "a/TopicA" 
//key is a/
//Value is TopicA

//second pass
Console.WriteLine("Creating Improved producer");


//!!!
//not sure if a/b/q/ goes in topic or schema map <- a/b/q/ from sample documentation -- default topic different syntax 
//!!!
var improved = new Meth.WrappedMethProducer("b-1.mattbroker.gfrhzv.c6.kafka.us-east-2.amazonaws.com:9092", "DefaultTopic" , test); // "WrappedLib");

//improved.AddBroker("AdditionalServerURL");

//var improved = new Meth.WrappedMethProducer("z-2.mattbroker.gfrhzv.c6.kafka.us-east-2.amazonaws.com:2181", "interestingTopic", test); // "WrappedLib");
//was testing with Roy we never could get my computer connected to AWS resource

//examples of creating params for addblock in a C# program

//how to create these params in a C# program -- there are multiple ways
string hashString = "deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff";
byte[] easyHash = Convert.FromHexString(hashString);

byte[] hash = new byte[] { 0xde, 0xad, 0xbe, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x1f, 0xff, 0xff, 0xff, 0xff, 0xff };
byte[] parent = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
byte[] weight = new byte[] { 0x26, 0x74 }; 


//there is probably a better way to deal with byte[] in C#, dont often use byte array as a data type
Dictionary<string, byte[]> updates = new Dictionary<string, byte[]>(){
    { "a/b", new byte[] {0x88} },
    { "q/17", new byte[] {0x12, 0x34} },
    { "q/18", new byte[] {0x56, 0x78 } },
    { "x/19", new byte[] { 0x99, 0x99 } }
};

var deletes = new List<string>() { "b/c" }; //["b/c"] for single, multiple ["b/c", "t/g"]

//there is a better way to do the batches data type, I need to figure it out
//this data type needs to be improved, need to discuss with Austin 
Dictionary<string, byte[]> batches = new Dictionary<string, byte[]>() {
    {"b/s", Convert.FromHexString("0000000000000000000000000000000000000000000000000000000000000001")},
    {"b/s/5", Convert.FromHexString("abcd")},
    {"b/s/4", Convert.FromHexString("de") }//this is wrong, just doing to get started -- need to update this datatype

};// ... need to figure out batches datatype, since it has subbatches
//batches included in message 0 "b/s": {"subbatch": 0x0000000000000000000000000000000000000000000000000000000000000001} in addblock
//batches and subbatch details not sent up until SendBatches called messages 5,6,7.... i think 

//new way
Dictionary<string, byte[]> subBatches = new Dictionary<string, byte[]>() {
    {"b/s", Convert.FromHexString("0000000000000000000000000000000000000000000000000000000000000001")}
};
int blockId = 1;
int itemCount = 2;
Dictionary<string, byte[]> batchUpdates = new Dictionary<string, byte[]>() {
    {"b/s/5", Convert.FromHexString("abcd")}
};

List<string> batchDeletes = new List<string>() { "b/s/4" };


//new way
Batch B = new Batch(easyHash, blockId, itemCount, subBatches, batchUpdates, batchDeletes);
//new way
await improved.SendBatch(B); 


//call add block with the variables from the example documentation -- messages 0 through 4
await improved.AddBlock(1337, easyHash, parent, weight, updates, deletes, batches ); //change batches param to list of new Batch data type
await improved.SendBatch(1, easyHash, updates, deletes, batches);


Thread.Sleep(10000);
Console.WriteLine("Flushing");
improved.Flush();
Console.WriteLine("Flush complete");

Console.WriteLine("Press any key to close");
Console.ReadKey();
return;//end program, don't run first draft code below

/// <summary>
/// Code below is not used anymore, just there for reference, but getting to where it has no use so will remove it soon. 
/// </summary>
#region unused
//first pass/ first draft below --this code wont run currently --- we return before this code is hit, no need to review unless you are curious about a nonwrapped kafka lib
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
#endregion unused