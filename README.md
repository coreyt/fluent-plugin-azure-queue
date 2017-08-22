# A  [Fluentd](http://github.com/fluent/fluentd) plugin to read from azure queues

## Dependencies

fluentd v.12

## Input: Configuration

    <source>
      @type azure_queue

      tag queue_input
      storage_account_name my_storage_account
      storage_access_key my_storage_access_key
      queue_name my_storage_queue
      fetch_interval 5     
      batch_size 10 
      lease_time 30
    </source>

**tag (required)**

The tag for the input

**storage_account_name (required)**

The storage account name

**storage_access_key (required)**

The storage account access key

**queue_name (required)**

The storage queue name

**message_key**

The the record key to put the message data into. Default 'message'

**fetch_interval**

How long to wait between getting messages from the queue. Default 5

**batch_size**

The maximum number of messages to pull from the queue at once. Default 10. Max 32

**lease_time**

The time to lease the messages for. Default 30

## Integration with Azure Event Hub

You can use an azure function to forward messages from event hubs to storage queues for easy ingestion by this gem. This is not recommended for high volumes, but should serve as a stop gap until a complete azure event hub gem is created.

```c#
using System;
using Microsoft.WindowsAzure.Storage.Queue;

public static void Run(string[] hubMessages, ICollector<string> outputQueue, TraceWriter log)
{
    foreach (string message in hubMessages)
    {
        int bytes = message.Length * sizeof(Char);
        if (bytes < 64000)
        {
            outputQueue.Add(message);
        }
        else 
        {
            log.Warning($"Message is larger than 64k with {bytes} bytes. Dropping message");
        }
    }
}
```