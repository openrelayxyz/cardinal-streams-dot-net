﻿using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Meth
{
    internal class AvroEncoder
    {
        public static byte[] Serialize(object obj)
        {
            using (MemoryStream resultStream = new MemoryStream())
            {
                //do we need a schema?
                var writer = new Avro.IO.BinaryEncoder(resultStream);
                {
                    writer.WriteString(obj.ToString());
                }
                var result = resultStream.ToArray();
                return result;
            }//don't need to flush mem stream since this is in a using block -- falls out of scope, auto garbage collected
        }

        //for batches message 5
        public static byte[] SerializeInt(int i)
        {
            using (MemoryStream resultStream = new MemoryStream())
            {
                var writer = new Avro.IO.BinaryEncoder(resultStream);
                {
                    writer.WriteInt(i);
                }
                var result = resultStream.ToArray();
                return result;
            }
        }
    }
}
