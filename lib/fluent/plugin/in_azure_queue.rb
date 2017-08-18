require 'fluent/input'
require 'azure/storage'
require 'base64'

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
    desc 'The time in seconds to sleep between fetching messages'
    config_param :fetch_interval, :integer, default: 5
    desc 'The number of messages to fetch during each request'
    config_param :batch_size, :integer, default: 10
    desc 'The the lease time on the messages in seconds'
    config_param :lease_time, :integer, default: 30

    def configure(conf)
      super
    end

    def start
      super
      if @batch_size > 32 || @batch_size < 1
        raise Fluent::ConfigError, "fluent-plugin-azure-queue: 'batch_size' parameter must be between 1 and 32: #{@batch_size}"
      end
      @queue_client = Azure::Storage::Client.create(
        :storage_account_name => @storage_account_name,
        :storage_access_key => @storage_access_key).queue_client
      log.debug("Succeeded to creating azure queue client")
      @running = true

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
        deleted = false
        begin
          if Time.now > @next_fetch_time
            @next_fetch_time += @fetch_interval
            log.trace("Fetching #{@batch_size} messages from azure queue", queue_name: @queue_name)
            messages = @queue_client.list_messages(@queue_name, @lease_time, { number_of_messages: @batch_size })
            log.trace("Recieved #{messages.count} messages from azure queue", queue_name: @queue_name)
            messages.each do |message|
              deleted = false
              message_content = Base64.decode64(message.message_text)
              router.emit(@tag, Fluent::Engine.now, { @message_key => message_content })
              log.trace("Emitted azure queue message", queue_name: @queue_name, id: message.id, content: message_content)
              @queue_client.delete_message(@queue_name, message.id, message.pop_receipt)
              log.trace("Deleted azure queue message", queue_name: @queue_name, id: message.id)
              deleted = true
            end
          else
            sleep 0.05
          end
        rescue => e
          if deleted
            log.warn(error: e)
            log.warn_backtrace(e.backtrace)
          else
            log.warn("Message emmitted but not deleted from azure queue", queue_name: @queue_name, error: e)
            log.warn_backtrace(e.backtrace)
          end
        end
      end

    end
  end
end