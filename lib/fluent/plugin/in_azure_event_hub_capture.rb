require 'fluent/input'
require 'azure/storage'
require "avro"

module Fluent
  class AzureEventHubCaptureInput < Input
    Fluent::Plugin.register_input('azure_event_hub_capture', self)

    desc 'Tag of the output events.'
    config_param :tag, :string
    desc 'The azure storage account name'
    config_param :storage_account_name, :string
    desc 'The azure storage account access key'
    config_param :storage_access_key, :string
    desc 'The container name'
    config_param :container_name, :string
    desc 'The the record key to put the message data into'
    config_param :message_key, :string, default: 'message'
    desc 'The time in seconds to sleep between fetching the blob list'
    config_param :fetch_interval, :integer, default: 30
    desc 'The the lease duration on the blob in seconds'
    config_param :lease_duration, :integer, default: 60

    def configure(conf)
      super
    end

    def start
      super
      if @lease_duration > 60 || @lease_duration < 15
        raise Fluent::ConfigError, "fluent-plugin-azure-queue: 'lease_duration' parameter must be between 15 and 60: #{@lease_duration}"
      end
      @blob_client = Azure::Storage::Client.create(
        :storage_account_name => @storage_account_name,
        :storage_access_key => @storage_access_key).blob_client
      @running = true

      @thread = Thread.new(&method(:run))
    end

    def shutdown
      log.debug("Begin in azure blob shutdown")
      @running = false
      @thread.join
      log.debug("Finish in azure blob shutdown")
      super
    end

    private

    def run
      log.debug("Begin running in azure blob")
      @next_fetch_time = Time.now
      while @running
        if Time.now > @next_fetch_time
          @next_fetch_time = Time.now + @fetch_interval
          begin
            blobs = @blob_client.list_blobs(@container_name)
            blobs = blobs.select { |b| b.properties[:lease_status] == "unlocked" }
            log.info("Found #{blobs.count} unlocked blobs", container_name: @container_name)
            # Blobs come back with oldest first
            blobs.each do |blob|
              ingest_blob(blob)
            end
          rescue => e
            log.warn(error: e)
            log.warn_backtrace(e.backtrace)
          end
        else
          sleep(@next_fetch_time - Time.now)
        end
      end
    end

    def ingest_blob(blob)
      begin
        lease_id = @blob_client.acquire_blob_lease(@container_name, blob.name, duration: @lease_duration)
        log.info("Blob Leased", blob_name: blob.name)
        blob, blob_contents = @blob_client.get_blob(@container_name, blob.name)
        emit_blob_messages(blob_contents)
        log.trace("Done Ingest blob", blob_name: blob.name)
        begin
          delete_blob(blob, lease_id)
          log.debug("Blob deleted", blob_name: blob.name)
        rescue Exception => e
          log.warn("Records emmitted but blob not deleted", container_name: @container_name, blob_name: blob.name, error: e)
          log.warn_backtrace(e.backtrace)
        end
      rescue Azure::Core::Http::HTTPError => e
        if e.status_code == 409
          log.trace("Blob already leased", blob_name: blob.name)
        elsif e.status_code == 404
          log.trace("Blob already deleted", blob_name: blob.name)
        else
          log.warn("Error occurred while ingesting blob", error: e)
          log.warn_backtrace(e.backtrace)
        end
      rescue Exception => e
        log.warn("Error occurred while ingesting blob", error: e)
        log.warn_backtrace(e.backtrace)
      end
    end

    def emit_blob_messages(blob_contents)
      buffer = StringIO.new(blob_contents)
      reader = Avro::DataFile::Reader.new(buffer, Avro::IO::DatumReader.new)
      event_stream = MultiEventStream.new
      begin
        reader.each do |record|
          time = Time.strptime(record["EnqueuedTimeUtc"], "%m/%d/%Y %r").to_i
          value = { @message_key => record["Body"] }
          event_stream.add(time, value)
        end
      rescue NoMethodError => e
        if e.message.include? "unpack"
          log.warn("Found 0 length block in blob")
        else
          throw
        end
      end
      router.emit_stream(@tag, event_stream)
    end

    def delete_blob(blob, lease_id)
      # Hack because 'delete_blob' doesn't support lease_id yet
      Azure::Storage::Service::StorageService.register_request_callback { |headers| headers["x-ms-lease-id"] = lease_id }
      @blob_client.delete_blob(@container_name, blob.name)
      Azure::Storage::Service::StorageService.register_request_callback { |headers| headers }
    end
  end
end