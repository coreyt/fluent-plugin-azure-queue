require "test_helper"
require "fluent/test/driver/input"
require "fluent/plugin/in_azure_event_hub_capture"

class AzureEventHubCaptureInputTest < Test::Unit::TestCase

  def setup
    Fluent::Test.setup
    if Fluent.const_defined?(:EventTime)
      #stub(Fluent::EventTime).now { @time }
    end
  end

  LIST_CONFIG = %[
    tag test_tag
    storage_account_name test_storage_account_name
    storage_access_key test_storage_access_key
    container_names test_container_name
    fetch_interval 1
  ]

  QUEUE_CONFIG = %[
    tag test_tag
    storage_account_name test_storage_account_name
    storage_access_key test_storage_access_key
    container_names test_container_name
    fetch_interval 1
    queue_lease_time 30
    queue_name test_queue_name
  ]

  def create_driver(conf = LIST_CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::AzureEventHubCaptureInput).configure(conf)
  end

  Struct.new("Blob", :name, :properties)

  def test_configure
    d = create_driver
    assert_equal 'test_tag', d.instance.tag
    assert_equal 'test_storage_account_name', d.instance.storage_account_name
    assert_equal 'test_storage_access_key', d.instance.storage_access_key
    assert_equal 'test_container_name', d.instance.container_names
    assert_equal 1, d.instance.fetch_interval
  end

  def setup_mocks(driver)
    blob_client = flexmock("blob_client")
    queue_client = flexmock("queue_client")
    flexmock(Azure::Storage::Common::Client, :create => nil)
    flexmock(Azure::Storage::Queue::QueueService, :new => queue_client)
    flexmock(Azure::Storage::Blob::BlobService, :new => blob_client)
    [blob_client, queue_client]
  end

  def test_list_no_blobs
    d = create_driver
    blob_client, queue_client = setup_mocks(d)
    blob_client.should_receive(:list_blobs).with(d.instance.container_names).and_return([]).once
    flexmock(d.instance).should_receive(:ingest_blob).never()
    d.run do
      sleep 1
    end
  end

  def test_list_two_blobs
    d = create_driver
    blobs = [Struct::Blob.new("test1", lease_status: "unlocked"), Struct::Blob.new("test2", lease_status: "unlocked")]
    blob_client, queue_client = setup_mocks(d)
    blob_client.should_receive(:list_blobs).with(d.instance.container_names).and_return(blobs).once
    plugin = flexmock(d.instance)
    plugin.should_receive(:ingest_blob).with(d.instance.container_names, blobs[0].name).once()
    plugin.should_receive(:ingest_blob).with(d.instance.container_names, blobs[1].name).once()
    d.run do
      sleep 1
    end
  end

  def test_queue_no_blobs
    d = create_driver(QUEUE_CONFIG)
    blob_client, queue_client = setup_mocks(d)
    queue_client.should_receive(:list_messages).with(
      d.instance.queue_name,
      d.instance.queue_lease_time,
      { number_of_messages: 32}).and_return([]).once
    flexmock(d.instance).should_receive(:ingest_blob).never()
    d.run do
      sleep 1
    end
  end

  def test_queue_two_blobs
    d = create_driver(QUEUE_CONFIG)
    blob_id1 = { "Name" => "test1", "Container" => "container1" }
    blob_id2 = { "Name" => "test2", "Container" => "container2" }
    blobs_id_messages = [ Struct::QueueMessage.new(1, 99, Base64.encode64(blob_id1.to_json)),
                          Struct::QueueMessage.new(2, 299, Base64.encode64(blob_id2.to_json))]
    blob_client, queue_client = setup_mocks(d)
    queue_client.should_receive(:list_messages).with(
      d.instance.queue_name,
      d.instance.queue_lease_time,
      { number_of_messages: 32}).and_return(blobs_id_messages).once
    plugin = flexmock(d.instance)
    plugin.should_receive(:ingest_blob).with(blob_id1["Container"], blob_id1["Name"]).once()
    plugin.should_receive(:ingest_blob).with(blob_id2["Container"], blob_id2["Name"]).once()
    queue_client.should_receive(:delete_message).with(d.instance.queue_name, blobs_id_messages[0].id, blobs_id_messages[0].pop_receipt).once
    queue_client.should_receive(:delete_message).with(d.instance.queue_name, blobs_id_messages[1].id, blobs_id_messages[1].pop_receipt).once
    d.run do
      sleep 1
    end
  end

  def test_ingest_blob
    d = create_driver
    blob = Struct::Blob.new("test1", lease_status: "unlocked")
    blob_client, queue_client = setup_mocks(d)
    plugin = flexmock(d.instance)
    lease_id = "123"
    blob_client.should_receive(:acquire_blob_lease).with(d.instance.container_names, blob.name, duration: d.instance.lease_duration).and_return(lease_id).once
    updated_blob = Struct::Blob.new("test1", lease_status: "locked")
    blob_contents = flexmock("blob_contents")
    blob_client.should_receive(:get_blob).with(d.instance.container_names, blob.name).and_return([updated_blob, blob_contents]).once
    plugin.should_receive(:emit_blob_messages).with(blob_contents).once
    blob_client.should_receive(:delete_blob).with(d.instance.container_names, updated_blob.name, lease_id: lease_id).once
    d.run do
      plugin.send(:ingest_blob, d.instance.container_names, blob.name)
    end
  end

  def test_emit_blob_messages
    d = create_driver
    blob_client, queue_client = setup_mocks(d)
    test_payload = flexmock("test_payload")
    buffer = flexmock("buffer")
    flexmock(StringIO).should_receive(:new).and_return(buffer)
    time = 1504030204
    time_string = Time.at(time).strftime("%m/%d/%Y %r")
    original_payload = {"key" => "value"}.to_json
    records = [ {"EnqueuedTimeUtc" => time_string, "Body" => original_payload } ]
    flexmock(Avro::DataFile::Reader).should_receive(:new).with(buffer, Avro::IO::DatumReader).and_return(records)
    d.run do
      d.instance.send(:emit_blob_messages, test_payload)
    end
    assert_equal(1, d.events.size)
    assert_equal(d.events[0], [d.instance.tag, time, { "message" => original_payload }])
  end
end