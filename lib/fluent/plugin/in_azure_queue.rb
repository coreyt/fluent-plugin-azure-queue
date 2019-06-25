require 'fluent/plugin/input'
require 'azure/storage/queue'
require 'azure/storage/common'
require 'concurrent'

module Fluent::Plugin
  class AzureQueueInput < Input
    Fluent::Plugin.register_input('azure_storage_queue', self)

    desc 'The tag to be used on the emitted events'
    config_param :tag, :string

    desc 'The Azure storage account name'
    config_param :storage_account_name, :string

    desc 'The Azure Storage Account access_key'
    config_param :storage_access_key, :string

    desc 'The Azure storage account queue_name'
    config_param :storage_queue_name, :string

    desc 'The the fluentd record key for the queue message payload. Optional. Default is message'
    config_param :message_key, :string, default: 'message'

    desc 'The duration of time in seconds a listed / found message is no longer visible to subsequent requests to the queue. in seconds. Basically how long should the thread wait until it either issues a delete request or lets the queue make the message visible again so the next worker will see it. See https://docs.microsoft.com/en-us/rest/api/storageservices/get-messages. Optional. Default is 300'
    config_param :visibility_timeout_time, :integer, default: 300

    desc 'The timeout interval in seconds to set on Azure storage queue service. Per Microsoft docs, if the Azure server timeout interval elapses before the Azure service has finished processing the request, the Azure service returns an error. The maximum timeout interval for Queue service operations is 30 seconds. See https://docs.microsoft.com/en-us/rest/api/storageservices/setting-timeouts-for-queue-service-operations. Optional. Default is 30. NOT IMPLEMENTED.'
    config_param :server_timeout_interval, :integer, default: 30

    desc 'Specifies the version of the operation to use for this request. For more information, see Versioning for the Azure Storage Services. See https://docs.microsoft.com/en-us/rest/api/storageservices/get-messages. Optional. Default is currently set by azure-storage-ruby gem at 2017-11-09. PARAM CONFIG NOT IMPLEMENTED.'
    config_param :ms_api_version, :string, default: ''

    desc 'Provides a client-generated, opaque value with a 1 KB character limit that is recorded in the analytics logs when storage analytics logging is enabled. See https://docs.microsoft.com/en-us/rest/api/storageservices/get-messages. Optional. Default is blank. NOT IMPLEMENTED.'
    config_param :ms_client_request_id, :string, default: ''

    desc 'The maximum size of the thread pool. Threads are responsible for doing the message GET, as well as the fluentd router.emit processing.'
    config_param :max_fetch_threads, :integer, default: 30

    desc 'A string to be prefixed to the user_agent on HTTP GETs the Azure Storage Queue. Optional. Default is blank.'
    config_param :user_agent_prefix, :string, default: ''


    def configure(conf)
      super
      # TODO none of the settings are going to be checked, so just end
    end

    def start
      super
      # from the microsoft example at https://github.com/Azure/azure-storage-ruby/blob/master/queue/README.md
      # @queue_client = Azure::Storage::Queue::QueueService.create(storage_account_name: <your account name>, storage_access_key: <your access key>)
      
      # from the original code
      #@queue_client = Azure::Storage::Queue::QueueService.create(
      #  :storage_account_name => @storage_account_name,
      #  :storage_access_key => @storage_access_key)
      @queue_client = Azure::Storage::Queue::QueueService.create(storage_account_name: @storage_account_name, storage_access_key: @storage_acces_key, user_agent_prefix: @user_agent_prefix)
      log.debug("Succeeded to creating azure queue client")
      @running = true

      @delete_pool = Concurrent::ThreadPoolExecutor.new(
        min_threads: 1,
        max_threads: @max_fetch_threads,
        max_queue: @max_fetch_threads,
      )
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      log.debug("Begin azure queue shutdown")
      @running = false
      @thread.join
      log.debug("Finish azure queue shutdown")
      super
    end

    private

    def run
      log.debug("Begin running azure queue")
      @next_fetch_time = Time.now
      while @running
        delete_futures = []
        begin
          if ((@delete_pool.queue_length) < @max_fetch_threads)
              Concurrent::Future.execute(executor: @delete_pool) do
                start = Time.now
                message = @queue_client.list_messages(@storage_queue_name, @visibility_timeout_time, { number_of_messages: 1 })[0]
                log.trace("Recieved 1 messages from azure queue", storage_queue_name: @storage_queue_name, id: message.id, time: Time.now - start)
                router.emit(@tag, Fluent::Engine.now, { @message_key => Base64.decode64(message.message_text)})
                start = Time.now
                @queue_client.delete_message(@storage_queue_name, message.id, message.pop_receipt)
                log.trace("Deleted azure queue message", storage_queue_name: @storage_queue_name, id: message.id, time: Time.now - start)
              end
          else
            log.trace("Not fetching more messages, already have #{@delete_pool.queue_length} messages to be deleted")
            sleep 0.5
          end
        rescue => e
          log.warn(error: e)
          log.warn_backtrace(e.backtrace)
        end
      end
    end
  end
end
