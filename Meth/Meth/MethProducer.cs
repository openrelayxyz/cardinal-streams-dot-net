//Matt Yale 2022 Feb
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using Avro;
using System.Text.RegularExpressions;

namespace Meth
{
    /// Not started yet -- ReOrg Milestone
    /// Reorg(number int64, hash types.Hash)
    /// ReorgDone(number int64, hash types.Hash)

    class WrappedMethProducer //custom class wrapping kafka library to add more data to the producer
    {
        private IProducer<byte[], byte[]> _Producer; //Producer is keyword, but internal class inacessible
        private string Topic;
        private string ReOrgTopic;
        private Dictionary<Regex, string> SchemaMap;
        private string brokerURL;            

        //Encapsolates a kafka producer, C# kafka lib has no way to store default topic in the producer... 
        public WrappedMethProducer(string kafkaBrokerURL, string topic, Dictionary<Regex, string> schemaMap, string specialname = null)
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

            _Producer = new ProducerBuilder<byte[], byte[]>(configuration.AsEnumerable()).Build();
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

        public byte[] CombineByteArrays(byte[] start, byte[] end)
        {
            var s = new MemoryStream();
            s.Write(start, 0, start.Length);
            s.Write(end, 0, end.Length);
            var array = s.ToArray();

            return array;
        }

        public byte[] AddPrefixByte(byte[] bArray, byte PreFixByte)
        {
            byte[] newArray = new byte[bArray.Length + 1];
            bArray.CopyTo(newArray, 1);
            newArray[0] = PreFixByte;
            return newArray;
        }

        public byte[] AddPostFixByte(byte[] bArray, byte PostFixByte)
        {
            byte[] newArray = new byte[bArray.Length + 1];
            bArray.CopyTo(newArray, 0);
            newArray[newArray.Length - 1] = PostFixByte;
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
        {                                                                                                                                                              //Eventually change this to List<Batch> batches -- find as is for now                                       
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

            string updatesstring = GetUpdatesCountStrings(updates, deletes); //update this method to be able to handle any letter combo currently only takes letters in the samples
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
            //messages.Add(msg0);
            //instead of adding it to list here, send it now, to make sure message 0 arrives first
            await SendMessageNow(msg0, "Message0", true); //true to block so message 0 sends first always
            //we want this to block so that message 0 is always first, other messages can be opened on new threads to keep things running in parallel 

            #endregion messageZero           

            AddUpdatesToMessages(hash, updates, messages);

            AddDeletesToMessages(hash, deletes, messages);

            Console.WriteLine("");
            Console.WriteLine("");
           
            SendAllMessagesNow(messages);
            return;
        }
        private async Task SendAllMessagesNow(List<Message<byte[], byte[]>> messageList)
        {
            Console.WriteLine("Sending all block messages now");
            int total = messageList.Count;

            await Task.Run(() =>
            {
                int count = 0;
                foreach (var message in messageList)
                {
                    string name = "Message " + ++count + " of " + total + " messages";
                    SendMessageNow(message, name);
                }
            });
            Console.WriteLine(" All messages queued to send");
        }

        private async Task SendMessageNow(Confluent.Kafka.Message<byte[], byte[]> message, string messageName = "", bool block = false)
        {
            if(!string.IsNullOrEmpty(messageName))
            {
                Console.WriteLine("Attempting to send message: " + messageName + " via kafka producer");
            }
            try
            {
                if (block)
                {
                    var deliveryResult = await _Producer.ProduceAsync(Topic, message);
                    Console.WriteLine("Delivery result status :" + deliveryResult.Status);
                }
                else
                {
                    await Task.Run(() =>
                    {
                         var result = _Producer.ProduceAsync(Topic, message); //do not block or wait for result
                         Console.WriteLine("Delivery result status :" + result.Status);
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("");
                Console.WriteLine("ProduceAsync threw an exception : Short Message " + ex.Message + "\n Long Message : " + ex.StackTrace);
                Console.WriteLine("");
            }
        }

        private void AddUpdatesToMessages(byte[] hash, Dictionary<string, byte[]> updates, List<Message<byte[], byte[]>> messages)
        {
            foreach (var u in updates)
            {
                bool foundKey = false;
                //if(SchemaMap.ContainsKey(letter.ToString()))
                    foreach(var reg in SchemaMap)
                    {
                    //extract method 
                        if (reg.Key.IsMatch(u.Key))
                        {
                            foundKey = true;
                        }
                    }
                if (!foundKey)
                {
                    continue; //skip sending message if not in schemaMap, go on to next entry in updates
                }
                var m = new Message<byte[], byte[]>();
                byte[] post = Encoding.UTF8.GetBytes(u.Key); //for end of byte array
                byte[] tmp = AddPrefixByte(hash, 0x03);

                var key = CombineByteArrays(tmp, post);

                m.Key = key;
                m.Value = u.Value; //Avro encoding not needed for updates messages
                messages.Add(m);

                Console.WriteLine("");
                Console.WriteLine("Update message created key =" + PrettyPrintByteArray(m.Key) + " Value =" + PrettyPrintByteArray(m.Value));
                Console.WriteLine("");
            } 
        }
        

        private void AddDeletesToMessages(byte[] hash, List<string> deletes, List<Message<byte[], byte[]>> messages)
        {
            //do we always send deletes no matter what? or do we need to check them against SchemaMap?
            foreach (var d in deletes)
            {
                var m = new Message<byte[], byte[]>();
                byte[] post = Encoding.UTF8.GetBytes(d); //for end of byte array
                byte[] tmp = AddPrefixByte(hash, 0x04);

                var key = CombineByteArrays(tmp, post);

                //no avro encoding needed for deletes messages
                m.Key = key;
                //m.Value = new byte[] { }; //Empty byte array for deletes---might not even need to set m.Value here
                messages.Add(m);

                Console.WriteLine("");
                Console.WriteLine("Deletes message created key =" + PrettyPrintByteArray(m.Key) + " Value =" + m.Value);
                Console.WriteLine("");
            }
        }

        private string GetNonSchemaUpdates(Dictionary<string, byte[]> updates) //do we need to do this for deletes also?
        {
            string result = "";
            foreach (var u in updates)
            {
                bool matchFound = false;
                foreach(var reg in SchemaMap)
                {                    
                    if(reg.Key.IsMatch(u.Key))
                    {
                        matchFound = true;
                    }
                    //TODO refactor to extract this into method
                }
                if(!matchFound)
                {
                    Console.WriteLine(" Update Key " + u.Key.ToString() + " not found in SchemaMap");
                    Console.WriteLine(" Adding key and value to updates string");
                    if (result.Equals(string.Empty))
                    {
                        result = result + "      \"" + u.Key.ToString() + "\": {\"value\": " + PrettyPrintByteArray(u.Value) + "}";
                    }
                    else
                    {
                        result = result + ",\n      \"" + u.Key.ToString() + "\": {\"value\": " + PrettyPrintByteArray(u.Value) + "}";
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
            result = "      \"" + first.Key.ToString() + "\": {\"subbatch\": " + PrettyPrintByteArray(first.Value) + "},\n";
            return result;
        }

        //gets the counts per update type 
        // Updates: { "a/b": 0x88, "q/17": 0x1234, "q/18": 0x5678, "x/19": 0x9999 }
        // "a/": {"count": 1},
        // "b/": {"count": 1},
        // "q/": {"count": 2},
        private string GetUpdatesCountStrings(Dictionary<string, byte[]> updates, List<string> deletes)
        {
            string result = "";
            foreach(var del in deletes)
            {
                int delCount = 0;
                foreach (var key in SchemaMap.Keys)
                {
                    if (key.IsMatch(del)) //if entry key matches regex expression in SchemaMap
                    {
                        //make sure we can't double count the same entry by accident
                        delCount++; //can more than one key match, if not, need to add here and then continue to next entry
                    }
                }
                if (delCount != 0)
                {
                    string toAdd = "      \"" + del + "\": { \"count\": " + delCount + "},\n"; result = result + toAdd;
                }
            }

            foreach (var entry in updates) //old way swapped inner and outer loops //foreach (var key in SchemaMap.Keys) //change keys to regex expressions
            {
                int currentKeyCount = 0;
                foreach (var key in SchemaMap.Keys)
                {
                    if( key.IsMatch(entry.Key) ) //if entry key matches regex expression in SchemaMap
                    {
                        //make sure we can't double count the same entry by accident
                        currentKeyCount++; //can more than one key match, if not, need to add here and then continue to next entry
                    }
                }
                if (currentKeyCount != 0) 
                {
                    string toAdd = "      \"" + entry.Key.ToString() + "\": { \"count\": " + currentKeyCount + "},\n"; result = result + toAdd; 
                }
            }            

            

            return result;
        }

        //TODO Reorg moved into another milestone
        public void ReOrg()//list of blocks? == a number and hash of block you are reorging to,
            //blocks will be sent via add block
            //once all blocks sent, call reorg done. 
        {
            //notify that a bunch of blocks coming with the reorg
            //addblock for all new blocks
            //send ReOrgDone
        }

        public void ReOrgDone()
        {
            //TODO
        }

        public async Task SendBatch(Batch batch) //is one header per batch a reasonable assumption?
        {
            Console.WriteLine("SendBatch called");
            //log batch values here? 
            // key : $PrefixByte.$BlockHash.$BatchId
            // value number of messages avroencoded 
            var messages = new List<Message<byte[], byte[]>>();
            var firstchunk = AddPrefixByte(batch.BlockHash, 0x01);
            byte[] key;
            var header = new Message<byte[], byte[]>();//will just call message 5 the header message, there might be more than one of these in real world examples
            string unencoded;
            byte[] encoded;

            foreach (var batchHeader in batch.SubBatches) //if there is only one batch header per Batch this can be removed
            {                
                key = CombineByteArrays(firstchunk, batchHeader.Value);
               
                header.Key = key;
                //if there are more than one header we might need to reorg the Batch datatype a bit for below line
                header.Value = AvroEncoder.SerializeInt(batch.ItemCount); //i think we need to store itemcount in subbatches ... will need to fix
                Console.WriteLine("");
                Console.WriteLine("SendBatch header message key : " + PrettyPrintByteArray(key));
                Console.WriteLine("SendBatch header value avro encoding : " + PrettyPrintByteArray(header.Value) + " unencoded : " + batch.ItemCount);

                messages.Add(header); // lets just send it now instead... 
            }

            foreach (var up in batch.Updates) // support multiple updates in batch 
            {
                var updates = new Message<byte[], byte[]>();//again just doing 1 message, need to extract into a loop that does it for all Updates
                firstchunk = AddPrefixByte(batch.BlockHash, 0x02);
                key = CombineByteArrays(firstchunk, batch.SubBatches.First().Value); // can there be more than 1?
                key = AddPostFixByte(key, 0x00); //go up by 1 for each update message? can you ++ a byte? 
                updates.Key = key;
                unencoded = "SubBatchMsg\n{\n  \"updates\": {\"" + up.Key + "\": " + PrettyPrintByteArray(up.Value) + "}\n}";
                encoded = AvroEncoder.Serialize(unencoded);
                updates.Value = encoded;
                Console.WriteLine("");
                Console.WriteLine("SendBatch updates message key : " + PrettyPrintByteArray(key));
                Console.WriteLine("SendBatch updates message value unencoded : " + unencoded);
                Console.WriteLine("SendBatch updates message value avroencoded " + PrettyPrintByteArray(encoded));
                Console.WriteLine("");
                messages.Add(updates);
            }

            foreach (var del in batch.Deletes)
            {
                var deletes = new Message<byte[], byte[]>();
                firstchunk = AddPrefixByte(batch.BlockHash, 0x02);
                key = CombineByteArrays(firstchunk, batch.SubBatches.First().Value);
                key = AddPostFixByte(key, 0x02); //go up by 1 for each update message? can you ++ a byte? 
                                                 //should this be 0x01 -- documentation seems off here --message 6 is 0x00 so id expect 7 to ve 0x01

                deletes.Key = key;
                unencoded = "SubBatchMsg\n{\n  \"delete\": [\"" + batch.Deletes.First() + "\"]\n}";
                encoded = AvroEncoder.Serialize(unencoded);
                deletes.Value = encoded;
                Console.WriteLine("");
                Console.WriteLine("SendBatch deletes message key : " + PrettyPrintByteArray(key));
                Console.WriteLine("SendBatch deletes message value unencoded : " + unencoded);
                Console.WriteLine("SendBatch deletes message value avroencoded " + PrettyPrintByteArray(encoded));
                Console.WriteLine("");
                messages.Add(deletes);
            }
                //send all messages in messages
                //error checking for sending across kafka
                return;
            
        }


        public void Flush()
        {
            _Producer.Flush();
        }
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
