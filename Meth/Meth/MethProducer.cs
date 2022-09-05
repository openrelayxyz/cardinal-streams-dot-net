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
    /// Not started yet -- left for milestone 1
    /// SendBatch(batchid types.Hash, delete []string, update map[string][] byte) (map[string][] Message, error)
    /// Reorg(number int64, hash types.Hash)
    /// ReorgDone(number int64, hash types.Hash)

    class WrappedMethProducer //custom class wrapping kafka library to add more data to the producer
    {
        private IProducer<string, string> _Producer; //Producer is keyword, but internal class inacessible
        private string Topic;
        private string ReOrgTopic;
        private Dictionary<string, string> SchemaMap;
        private string brokerURL;            

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

        /// <summary>
        /// Returns the number of brokers added, even if they have been added for a second time
        /// </summary>
        /// <param name="newServer"> URL of the broker to be added </param>
        /// <returns></returns>
        public int AddBroker(string newServer)
        {
            return _Producer.AddBrokers(newServer);
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
        /// <param name="updates"> Example :  { "a/b": 0x88, "q/17": 0x1234, "q/18": 0x5678, "x/19": 0x9999 } </param> //x/19 in updates, but not mapped
        /// <param name="deletes"> Examples :   ["b/c"] or ["b/c", "t/g"] </param>
        /// <param name="batches"> Example: Subbatches: {"b/s": 0x0000000000000000000000000000000000000000000000000000000000000001} Subbatch details: Updates: { "b/s/5": 0xabcd } Deletes: ["b/s/4"]</param>
        ///    batches param type might need tweeking since there are subbatches? 
        /// <returns></returns>
        public async Task AddBlock(int number, byte[] hash, byte[] parentHash, byte[] weight, Dictionary<string, byte[]> updates, List<string> deletes, Dictionary<string, byte[]> batches)
        {
            Console.WriteLine("Adding Block for topic " + Topic + " and producer " + _Producer.Name);
            var messages = new List<Message<byte[], byte[]>>();
            Console.WriteLine("");
            Console.WriteLine("");
            #region messageZero
            //message 0 key is prefix 00 with hashbyte array as key 
            //todo make this a method -- go from procedural to object
            var msg0 = new Message<byte[], byte[]>();
            msg0.Key = AddPrefixByte(hash, 0x00);
            Console.WriteLine(" Message 0 Key is " + PrettyPrintByteArray(msg0.Key));

            string num = "\"num\": " + number + ",\n  ";
            string w = "\"weight\": " + PrettyPrintByteArray(weight) + ",\n  ";
            string p = "\"parent\": " + PrettyPrintByteArray(parentHash) + ",\n  ";

            string updatesstring = GetUpdatesCountStrings(updates); //update this method to be able to handle any letter combo currently only takes letters in the samples
            //add subbatch to updates string
            string subbatch = GetSubBatchString(batches);
            updatesstring = updatesstring + subbatch;
            //add values of updates that are not in schema map to updates string
            string nonSchemaUpdates = GetNonSchemaUpdates(updates);  //x/19 from example
            updatesstring = updatesstring + nonSchemaUpdates; //these are entries in updates not specified in schema map that spec says go into message 0
            string up = "\"updates\": {\n" + updatesstring + " }\n}\n";

            string nonEncoded = "Batch:\n{\n  " + num + w + p + up;
            Console.WriteLine("Unencoded message 0 looks like this : \n\n" + nonEncoded);

            byte[] encoded = AvroEncoder.Serialize(nonEncoded);
            Console.WriteLine(" Avro Encoded message 0 : " + PrettyPrintByteArray(encoded));
            msg0.Value = encoded;
            //instead of adding it to list here, send it now, to make sure message 0 arrives first
            messages.Add(msg0);
            //instead of adding it to list here, send it not, to make sure message 0 arrives first
            #endregion messageZero           

            AddUpdatesToMessages(hash, updates, messages);

            AddDeletesToMessages(hash, deletes, messages);
            //For each message in messages send -- track any failures -- only log failures
            //for message in messages  -- send -- first lets send a single message below as a test for AddProducer
            Console.WriteLine("");
            Console.WriteLine("");
            //test message that we never got to send
            var message = new Message<string, string>(); //is this supposed to be the schema map?
                                                         //pretty sure this is what kafka sends, need to nail down what structure this is
                                                         //has to be created by params somehow, a real example would likely be helpful 
            
            
            message.Key = "Key-8-6---Roy";
            message.Value = "TestValue-8-6---Roy";
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

        private void AddUpdatesToMessages(byte[] hash, Dictionary<string, byte[]> updates, List<Message<byte[], byte[]>> messages)
        {
            foreach (var u in updates)
            {
                bool foundKey = false;
                //updates keys can have multiple letters, we need to check that if the updates key is included in the SchemaMap
                foreach(var letter in u.Key)
                {
                    if(SchemaMap.ContainsKey(letter.ToString()))
                    {
                        foundKey = true;
                    }
                }
                if(!foundKey)
                {
                    continue; //skip sending message if not in schemaMap, go on to next entry in updates
                }
                var m = new Message<byte[], byte[]>();
                byte[] post = Encoding.UTF8.GetBytes(u.Key); //for end of byte array
                byte[] tmp = AddPrefixByte(hash, 0x03);

                var s = new MemoryStream();
                s.Write(tmp, 0, tmp.Length);
                s.Write(post, 0, post.Length);
                var key = s.ToArray();

                m.Key = key;
                m.Value = u.Value; //Avro encoding not needed for updates messages
                messages.Add(m);

                Console.WriteLine("Update message created key =" + PrettyPrintByteArray(m.Key) + " Value =" + PrettyPrintByteArray(m.Value));
            }
        }

        private void AddDeletesToMessages(byte[] hash, List<string> deletes, List<Message<byte[], byte[]>> messages)
        {
            foreach (var d in deletes)
            {
                var m = new Message<byte[], byte[]>();
                byte[] post = Encoding.UTF8.GetBytes(d); //for end of byte array
                byte[] tmp = AddPrefixByte(hash, 0x04);

                var s = new MemoryStream();
                s.Write(tmp, 0, tmp.Length);
                s.Write(post, 0, post.Length);
                var key = s.ToArray();
                //no avro encoding needed for deletes messages
                m.Key = key;
                //m.Value = new byte[] { }; //Empty byte array for deletes---might not even need to set m.Value here
                messages.Add(m);

                Console.WriteLine("Deletes message created key =" + PrettyPrintByteArray(m.Key) + " Value =" + m.Value);

            }
        }


        private string GetNonSchemaUpdates(Dictionary<string, byte[]> updates)
        {
            string result = "";
            foreach (var u in updates)
            {
                //updates keys can have multiple letters, we need to check that if the updates key is included in the SchemaMap
                foreach (var letter in u.Key)
                {
                    if (!SchemaMap.ContainsKey(letter.ToString()))
                    {
                        if (!letter.Equals("/") && char.IsLetter(letter)) //this is a letter and not a number or a slash
                        {
                            Console.WriteLine(" Update Key " + u.Key.ToString() + " not found in SchemaMap");
                            Console.WriteLine(" Adding key and value to updates string");
                            if (result.Equals(string.Empty))
                            {
                                result = result + "\"" + u.Key.ToString() + "\": {\"value\": " + PrettyPrintByteArray(u.Value) + "}";
                            }
                            else
                            {
                                //if there are more than one we need a comma on new lines... TODO
                                result = result + ",\n\"" + u.Key.ToString() + "\": {\"value\": " + PrettyPrintByteArray(u.Value) + "}";
                            }
                        }
                    }
                }
            }
            result = result + "\n"; //add newline to last entry without comma
            return result;
        }

        private string GetSubBatchString(Dictionary<string, byte[]> batches) //will need to update this data type
        {
            string result = "";
            //TODO update datatype, and function
            //batches datatype will change, but for now, just use first key in dictionary
            var first = batches.First();
            result = "\"" + first.Key.ToString() + "\": {\"subbatch\": " + PrettyPrintByteArray(first.Value) + "},\n";
            return result;
        }

        //gets the counts per update type 
        // Updates: { "a/b": 0x88, "q/17": 0x1234, "q/18": 0x5678, "x/19": 0x9999 }
        // "a/": {"count": 1},
        // "b/": {"count": 1},
        // "q/": {"count": 2},
        private string GetUpdatesCountStrings(Dictionary<string, byte[]> updates)
        {
            string result = "";
            foreach (var key in SchemaMap.Keys)
            {
                int currentKeyCount = 0;
                foreach (var entry in updates)
                {
                    if(entry.Key.Contains(key.First()))
                    {
                        currentKeyCount++;
                    }
                }
                if (currentKeyCount != 0) 
                {
                    string toAdd = "\"" + key.ToString() + "/\": { \"count\": " + currentKeyCount + "},\n"; result = result + toAdd; 
                }
            }            
            //old hardcoded way for reference -- this worked, but did not handle every letter
            //if (aCount != 0) { string a = "a/\": { \"count\": " +aCount+"},}\n"; result = result + a; }
            //if (bCount != 0) { string b = "b/\": { \"count\": " +bCount+"},}\n"; result = result + b; }
            //if (qCount != 0) { string q = "q/\": { \"count\": " +qCount+ "},}\n"; result = result + q; }

            return result;
        }

        //TODO Complete these 3
        public void ReOrg()//list of blocks? 
        {
            //notify that a bunch of blocks coming with the reorg
            //addblock for all new blocks
            //send ReOrgDone
        }

        public void ReOrgDone()
        {
            //TODO
        }

        public async Task SendBatch(int batchId, byte[] hash, Dictionary<string, byte[]> updates, List<string> deletes, Dictionary<string, byte[]> batches)
        {
            //TODO
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
