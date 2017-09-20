# A  [Fluentd](http://github.com/fluent/fluentd) plugin to read from azure queues and event hubs
[![Build Status](https://travis-ci.org/sbonebrake/fluent-plugin-azure-queue.svg?branch=master)](https://travis-ci.org/sbonebrake/fluent-plugin-azure-queue)

This gem consists of two fluentd input plugins, azure_queue and azure_event_hub_capture.
The azure queue input plugin performs at about 30 messages/second in my tests.
If you need more throughput from event hubs, I suggest using the event hub capture plugin.

## Dependencies

fluentd v.12

## azure_queue Input Plugin

### Input: Configuration

    <source>
      @type azure_queue

      tag queue_input
      storage_account_name my_storage_account
      storage_access_key my_storage_access_key
      queue_name my_storage_queue
      fetch_interval 5
      lease_duration 30
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

**lease_duration**

The time to lease the messages for. Default 300

**max_fetch_threads**

The maximum number of threads to fetch and delete queue messages with. Default 30

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
## azure_event_hub_capture Input Plugin
This plugin is designed to work with blobs stored to a container via [Azure Event Hubs Capture](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview)

**Warning:** This plugin will delete the blobs after emitting the contents in fluentd.

### Input: Configuration

    <source>
      @type azure_event_hub_capture

      tag event_hub_input
      storage_account_name my_storage_account
      storage_access_key my_storage_access_key
      container_names my_capture_container
      fetch_interval 30
      lease_duration 30
    </source>

**tag (required)**

The tag for the input

**storage_account_name (required)**

The storage account name

**storage_access_key (required)**

The storage account access key

**container_names (required)**

The capture container names, comma separated.

**message_key**

The the record key to put the message data into. Default 'message'

**fetch_interval**

The time in seconds to sleep between fetching the blob list. Default 30

**lease_duration**

The time to lease the messages for. Default 60
