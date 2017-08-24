require 'fluent/input'
require 'azure/storage'
require 'concurrent'

module Fluent
  class AzureQueueInput < Input
    Fluent::Plugin.register_input('azure_queue', self)

    desc 'Tag of the output events.'
    config_param :tag, :string
    desc 'The azure storage account name'
    config_param :storage_account_name, :string
    desc 'The azure storage account access key'
    config_param :storage_access_key, :string
    desc 'The azure storage account queue name'
    config_param :queue_name, :string
    desc 'The the record key to put the message data into'
    config_param :message_key, :string, default: 'message'
    desc 'The the lease time on the messages in seconds'
    config_param :lease_time, :integer, default: 300
    desc 'The maximum number of threads to fetch messages'
    config_param :max_fetch_threads, :integer, default: 30

    def configure(conf)
      super
    end

    def start
      super
      @queue_client = Azure::Storage::Client.create(
        :storage_account_name => @storage_account_name,
        :storage_access_key => @storage_access_key).queue_client
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
                message = @queue_client.list_messages(@queue_name, @lease_time, { number_of_messages: 1 })[0]
                log.trace("Recieved 1 messages from azure queue", queue_name: @queue_name, id: message.id, time: Time.now - start)
                router.emit(@tag, Fluent::Engine.now, { @message_key => Base64.decode64(message.message_text)})
                start = Time.now
                @queue_client.delete_message(@queue_name, message.id, message.pop_receipt)
                log.trace("Deleted azure queue message", queue_name: @queue_name, id: message.id, time: Time.now - start)
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