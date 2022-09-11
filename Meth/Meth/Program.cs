﻿
using Meth;  //only thing you need to pull in to use lib,
using System.Text.RegularExpressions;
//or you could have the project itself be a dependancy in VS


Console.WriteLine("Hello, World!");

Dictionary<string, string> schemaMap = new Dictionary<string, string>(); //schema map param
schemaMap.Add(@"a/", "TopicA");
schemaMap.Add(@"b/", "TopicB"); //topics will be regex... lets get some examples of that soon in the code
schemaMap.Add(@"q/", "TopicQ");
//"a/b/q/" -- inside schema map "a/TopicA" 
//key is a/
//Value is TopicA

//Lets make this a Dictionary of type regex string instead
Dictionary<Regex, string> newSchemaMap = new Dictionary<Regex, string>();
newSchemaMap.Add(new Regex("^(a/).*"), "TopicA");
newSchemaMap.Add(new Regex("^(b/).*"), "TopicB");
newSchemaMap.Add(new Regex("^(q/).*"), "TopicQ");


//second pass
Console.WriteLine("Creating Improved producer");

var improved = new Meth.WrappedMethProducer("b-1.mattbroker.gfrhzv.c6.kafka.us-east-2.amazonaws.com:9092", "DefaultTopic" , newSchemaMap); // "WrappedLib");

//improved.AddBroker("AdditionalServerURL");

//var improved = new Meth.WrappedMethProducer("z-2.mattbroker.gfrhzv.c6.kafka.us-east-2.amazonaws.com:2181", "interestingTopic", test); // "WrappedLib");
//was testing with Roy we never could get my computer connected to AWS resource

//examples of creating params for addblock in a C# program

//how to create these params in a C# program -- there are multiple ways
string hashString = "deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff";
byte[] easyHash = Convert.FromHexString(hashString);
//this is the easy way, but the hashstring does not need prefix of 0x which might be confusing to people that 
//use byte arrays frequently

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
/// ***old way, see new way below, easier but split up.***
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
int itemCount = 2;//count does not include subbatch/header
Dictionary<string, byte[]> batchUpdates = new Dictionary<string, byte[]>() {
    {"b/s/5", Convert.FromHexString("abcd")}
};

List<string> batchDeletes = new List<string>() { "b/s/4" };

//call add block with the variables from the example documentation -- messages 0 through 4
//if sending multiple blocks back to back you might open a new thread for each one to prevent message 0 from blocking other blocks
await improved.AddBlock(1337, easyHash, parent, weight, updates, deletes, batches ); //change batches param to list of new Batch data type
//await improved.SendBatch(1, easyHash, updates, deletes, batches);
//new way
Batch B = new Batch(easyHash, blockId, itemCount, subBatches, batchUpdates, batchDeletes);
//new way
await improved.SendBatch(B); //messages 5 through 7


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

int hashId = 1234;
//first pass/ first draft below --this code wont run currently --- we return before this code is hit, no need to review unless you are curious about a nonwrapped kafka lib
//right now an empty dictionary
Console.WriteLine("Creating Walter White");
var WalterWhite = Meth.MethProducer.NewProducer("URL", "none", schemaMap, "WalterWhite");

Console.WriteLine("Creating Jesse Pinkman");
var JessePinkman = Meth.MethProducer.NewProducer("URL44", "othertopics", schemaMap, "JessePinkman");

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