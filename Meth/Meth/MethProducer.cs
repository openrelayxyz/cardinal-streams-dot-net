//Matt Yale 2022 Feb
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using Avro;

namespace Meth
{

    //NewProducer(kafkaBrokerURL, defaultTopic string, schema map[string] string) 
    // changing to dictionary data type, key value pairs

    //AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, 
    // updates map[string][] byte, deletes map[string]struct{ }, batches map
    //[string]types.Hash)

    /*
    BlockHash: A 32 byte identifier unique to this message payload. --unique
    Number: A 64 bit unsigned integer indicating the sequence number of this payload.
    Parent: The $BlockHash of the block preceding this one.
    Weight: A 256 bit unsigned indicator indicating the weight of this payload. If this is not meaningful for a given application, it can duplicate $Number.
     */


    /// Not started yet
    /// SendBatch(batchid types.Hash, delete []string, update map[string][] byte) (map[string][] Message, error)
    /// 
    ///
    /// Reorg(number int64, hash types.Hash)
    /// ReorgDone(number int64, hash types.Hash)

    class WrappedMethProducer //custom class wrapping kafka library to add more data to the producer
    {
        private IProducer<string, string> _Producer; //Producer is keyword, but internal class inacessible
        private string Topic;
        private string ReOrgTopic;
        private Dictionary<string, string> SchemaMap;
        private string brokerURL; //expose to constructor

        //private Dictionary<int, batchInfo>  pendingBatches;   //Map batchid -> batchInfo -- not sure what type batchInfo is
        //


        //from go impl
        //type batchInfo struct
        //{
        // topic string
        //  block types.Hash   -- types.Hash equivalent in C#? 32 byte array
        //}               

        //Encapsolates a kafka producer, C# kafka lib has no way to store default topic in the producer... 
        public WrappedMethProducer(string kafkaBrokerURL, string topic, Dictionary<string, string> schemaMap, string specialname = null)
        {
            var configuration = new ProducerConfig()
            {
                BootstrapServers = kafkaBrokerURL, //I think you can add more than one broker or server to a producer, will add that as an auxilary method
                ClientId = specialname ?? "Meth2022.V1", //might come in use if this continues evolving, telling where the messages came from
                MessageTimeoutMs = 30000, //keeps flush from blocking more than 10 seconds, just to make testing easier when hitting server urls that dont exist
                EnableDeliveryReports = true,
                MessageSendMaxRetries = 3,
                //other config things?                
            };

            _Producer = new ProducerBuilder<string, string>(configuration.AsEnumerable()).Build();
            Topic = topic;
            SchemaMap = schemaMap;
            brokerURL = kafkaBrokerURL;
        }

        public void AddBroker(string newServer)
        {
            _Producer.AddBrokers(newServer);
        }


        public byte[] AddPrefixByte(byte[] bArray, byte PreFixByte)
        {
            byte[] newArray = new byte[bArray.Length + 1];
            bArray.CopyTo(newArray, 1);
            newArray[0] = PreFixByte;
            return newArray;
        }

        public string PrettyPrintByteArray(byte[] array)
        {
            return "0x" + Convert.ToHexString(array);
        }

        /// <summary>
        ///  Add Block Meth API call
        /// </summary>
        /// <param name="number"> plain int example 1337</param>
        /// <param name="hash"> Example : 0xdeadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff hexidecimal hash?</param>
        /// <param name="parentHash"> Example: 0x0000000000000000000000000000000000000000000000000000000000000000 hexidecimal hash? </param>
        /// <param name="weight"> 0x2674</param>
        /// <param name="updates"> Example :  { "a/b": 0x88, "q/17": 0x1234, "q/18": 0x5678 } </param>
        /// <param name="deletes"> Examples :   ["b/c"] or ["b/c", "t/g"] </param>
        /// <param name="batches"> Example: Subbatches: {"b/s": 0x0000000000000000000000000000000000000000000000000000000000000001} Subbatch details: Updates: { "b/s/5": 0xabcd } Deletes: ["b/s/4"]</param>
        ///    batches param type might need tweeking since there are subbatches? 
        /// <returns></returns>
        public async Task AddBlock(int number, byte[] hash, byte[] parentHash, byte[] weight, Dictionary<string, byte[]> updates, List<string> deletes, Dictionary<string, byte[]> batches)
        {
            Console.WriteLine("Adding Block for topic " + Topic + " and producer " + _Producer.Name);

            //message 0 key is prefix 00 with hashbyte array as key 
            //todo make this a method -- go from procedural to object
            var msg0 = new Message<byte[], string>();
            msg0.Key = AddPrefixByte(hash, 0x00);
            Console.WriteLine(" Message 0 Key is " + PrettyPrintByteArray(msg0.Key));

            string num = "\"num\": " + number + ",\n  ";
            string w = "\"weight\": " + PrettyPrintByteArray(weight) + ",\n  ";
            string p = "\"parent\": " + PrettyPrintByteArray(parentHash) + ",\n  ";

            string updatesstring = GetUpdatesCountStrings(updates);
            string up = "\"updates\": {\n" + updatesstring + " }\n}\n";

            string nonEncoded = "Batch:\n{\n  " + num + w + p + up;
            Console.WriteLine("Unencoded message 0 looks like this : \n\n" + nonEncoded);

            string aandb = "a/b";
            byte[] aandbBA = Encoding.UTF8.GetBytes(aandb);
            Console.WriteLine(Convert.ToHexString(aandbBA));


            //avro encoding step needed
            msg0.Value = nonEncoded;

            
           
            //create messages
            var outStream = new Avro.IO.ByteBufferOutputStream();
            var encoder = new Avro.IO.BinaryEncoder(outStream);
            //avro encode
            
            encoder.WriteString(nonEncoded); //need to come back to this, not sure how to get encoded value back out
            Console.WriteLine("Encoded msg 0  " + nonEncoded);
            
            //send msg0 

            //prep and send messages associated with msg0


            //For each match create message, encode, and send (Avro Encoded)

            var message = new Message<string, string>(); //is this supposed to be the schema map?
                                                         //pretty sure this is what kafka sends, need to nail down what structure this is
                                                         //has to be created by params somehow, a real example would likely be helpful 
            message.Key = "Key-7-20---Roy";
            message.Value = "Test7-20---Roy";
            //change to Produce Async, capture errors and log them --- 

            //have this be a new Task on it's own thread-- that way things dont get blocked
            try
            {
                var deliveryResult = await _Producer.ProduceAsync(Topic, message);

                Console.WriteLine("Delivery result status :" + deliveryResult.Status);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ProduceAsync threw an exception : Short Message " + ex.Message + "\n Long Message : " + ex.StackTrace);
            }
            return;
        }

        //get

        //gets the counts per update type 
        // Updates: { "a/b": 0x88, "q/17": 0x1234, "q/18": 0x5678 }
        // "a/": {"count": 1},
        // "b/": {"count": 1},
        // "q/": {"count": 2},
        private string GetUpdatesCountStrings(Dictionary<string, byte[]> updates)
        {
            int aCount =0;
            int bCount =0;
            int qCount =0;
            string result = "";
            foreach( var entry in updates)
            {
                if (entry.Key.Contains("a"))
                {
                    aCount++;
                }
                if (entry.Key.Contains("b"))
                {
                    bCount++;
                }
                if (entry.Key.Contains("q"))
                {
                    qCount++;
                }
            }
            if (aCount != 0) { string a = "a/\": { \"count\": " +aCount+"},}\n"; result = result + a; }
            if (bCount != 0) { string b = "b/\": { \"count\": " +bCount+"},}\n"; result = result + b; }
            if (qCount != 0) { string q = "q/\": { \"count\": " +qCount+ "},}\n"; result = result + q; }

            return result;
        }

        public void ReOrg()
        {
            //notify that a bunch of blocks coming with the reorg
            //addblock for all new blocks
            //send ReOrgDone
        }

        public void ReOrgDone()
        {

        }

        public void Flush()
        {
            _Producer.Flush();
        }
        //methods here would be called on instances of the class
        //addblock -- has access to the topic and schema map, unlike vanilla kafka lib
        //reorg etc etc
    }


    //just for reference, below code no longer needed to be used
    class MethProducer //using only kafka library -- just used for testing and figuring out the lib
    {
        //not asked for in spec doc, could be useful though
        public static void AddBroker(IProducer<string, string> theProducer, string newServer)
        {
            theProducer.AddBrokers(newServer); //does not look like there is a way to remove brokers, only add new ones
        }

        // not void, not sure of return type yet, likely custom type, unsure of param types also, need to look at go implementation 
        public static void AddBlock(int hash, IProducer<string, string> theProducer, string topic)
        {
            Console.WriteLine("Adding Block for topic " + topic + " and producer " + theProducer.Name);
            //there is much more to this... for now just testing
            //a little different, pass the producer you want to AddBlock, need more param clarification 
            //can have an AddBlock that takes a list of Producers as well if needed to add a block to multiple producers
            var message = new Message<string, string>(); //is this supposed to be the schema map?
            theProducer.Produce(topic, message);
            return;
        }

        //topic and schema map params to be removed once confirming with austin current strategy ok. 
        //problem, C# kafka library has no way to set default topic in the producer
        public static IProducer<string, string> NewProducer(string kafkaBrokerURL, string topic, Dictionary<string, string> schemaMap, string specialname = null)
        {
            //old config attempt
            //IConfiguration configuration = new ConfigurationBuilder()
            //    .Build();
            //                .Properties.Add(topic, schemaMap)

            var configuration = new ProducerConfig()
            {
                BootstrapServers = kafkaBrokerURL, //I think you can add more than one broker or server to a producer, will add that as an auxilary method
                ClientId = specialname ?? "Meth2022.V1", //might come in use if this continues evolving, telling where the messages came from
                MessageTimeoutMs = 10000, //keeps flush from blocking more than 10 seconds, just to make testing easier when hitting server urls that dont exist
                //other config things?                
            };

            //need to figure out how to get topic and schema map params into the config still

            var producer = new ProducerBuilder<string, string>(configuration.AsEnumerable()).Build();
            //Solved with BootstrapServers in config... 
            //%5|1652645662.511|CONFWARN|rdkafka#producer-1| [thrd:app]: No `bootstrap.servers` configured: client will not be able to connect to Kafka cluster -- current error when building with this config
            Console.WriteLine("Producer created");
            Console.WriteLine("Adding broker " + kafkaBrokerURL);

            return producer;
        }
        
    }
}
