require 'fluent/test'
require 'fluent/plugin/in_azure_event_hub_capture'
require 'flexmock/test_unit'
require 'fluent/input'

class AzureEventHubCaptureInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    if Fluent.const_defined?(:EventTime)
      stub(Fluent::EventTime).now { @time }
    end
  end

  CONFIG = %[
    tag test_tag
    storage_account_name test_storage_account_name
    storage_access_key test_storage_access_key
    container_names test_container_name
    fetch_interval 1
  ]

  def create_driver(conf = CONFIG)
    d = Fluent::Test::InputTestDriver.new(Fluent::AzureEventHubCaptureInput)
    d.configure(conf)
    d
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
    client = flexmock("client", :blob_client => blob_client)
    flexmock(Azure::Storage::Client, :create => client)
    blob_client
  end

  def test_no_blobs
    d = create_driver
    blob_client = setup_mocks(d)
    blob_client.should_receive(:list_blobs).with(d.instance.container_names).and_return([]).once
    flexmock(d.instance).should_receive(:ingest_blob).never()
    d.run do
      sleep 1
    end
  end

  def test_two_blobs
    d = create_driver
    blobs = [Struct::Blob.new("test1", lease_status: "unlocked"), Struct::Blob.new("test2", lease_status: "unlocked")]
    blob_client = setup_mocks(d)
    blob_client.should_receive(:list_blobs).with(d.instance.container_names).and_return(blobs).once
    plugin = flexmock(d.instance)
    plugin.should_receive(:ingest_blob).with(d.instance.container_names, blobs[0]).once()
    plugin.should_receive(:ingest_blob).with(d.instance.container_names, blobs[1]).once()
    d.run do
      sleep 1
    end
  end

  def test_ingest_blob
    d = create_driver
    blob = Struct::Blob.new("test1", lease_status: "unlocked")
    blob_client = setup_mocks(d)
    plugin = flexmock(d.instance)
    lease_id = "123"
    blob_client.should_receive(:acquire_blob_lease).with(d.instance.container_names, blob.name, duration: d.instance.lease_duration).and_return(lease_id).once
    updated_blob = Struct::Blob.new("test1", lease_status: "locked")
    blob_contents = flexmock("blob_contents")
    blob_client.should_receive(:get_blob).with(d.instance.container_names,  blob.name).and_return([updated_blob, blob_contents]).once
    plugin.should_receive(:emit_blob_messages).with(blob_contents).once
    plugin.should_receive(:delete_blob).with(d.instance.container_names, updated_blob, lease_id).once
    d.run do
      plugin.send(:ingest_blob, d.instance.container_names, blob)
    end
  end

  def test_emit_blob_messages
    d = create_driver
    setup_mocks(d)
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
    assert_equal(1, d.emits.size)
    d.expect_emit(d.instance.tag, time, { "message" => original_payload })
  end
end