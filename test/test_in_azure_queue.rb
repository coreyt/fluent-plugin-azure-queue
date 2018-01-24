require 'fluent/test'
require 'fluent/plugin/in_azure_queue'
require 'flexmock/test_unit'

class AzureQueueInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @time = Time.parse("2017-08-17 18:03:27 UTC")
    Fluent::Engine.now = @time
    if Fluent.const_defined?(:EventTime)
      stub(Fluent::EventTime).now { @time }
    end
  end

  CONFIG = %[
    tag test_tag
    storage_account_name test_storage_account_name
    storage_access_key test_storage_access_key
    queue_name test_queue_name
  ]

  def create_driver(conf = CONFIG)
    d = Fluent::Test::InputTestDriver.new(Fluent::AzureQueueInput)
    d.configure(conf)
    d
  end

  def test_configure
    d = create_driver
    assert_equal 'test_tag', d.instance.tag
    assert_equal 'test_storage_account_name', d.instance.storage_account_name
    assert_equal 'test_storage_access_key', d.instance.storage_access_key
    assert_equal 'test_queue_name', d.instance.queue_name
  end

  Struct.new("QueueMessage", :id, :pop_receipt, :message_text)

  def setup_mocks(driver, messages)
    queue_client = flexmock("queue_client")
    queue_client.should_receive(:list_messages).with(
      driver.instance.queue_name,
      driver.instance.lease_time,
      { number_of_messages: 1}).and_return(messages).once
    flexmock(Azure::Storage::Queue::QueueService, :create => queue_client)
    queue_client
  end

  def test_one_message
    d = create_driver
    messages = [ Struct::QueueMessage.new(1, 99, Base64.encode64("test line"))]
    queue_client = setup_mocks(d, messages)
    queue_client.should_receive(:delete_message).with(d.instance.queue_name, messages[0].id,messages[0].pop_receipt).once
    d.run do
      sleep 1
    end
    assert_equal(1, d.emits.size)
    d.expect_emit(d.instance.tag, @time, { "message" => "test line" })
  end

  def test_no_messages
    d = create_driver
    messages = []
    queue_client = setup_mocks(d, messages)
    d.run do
      sleep 1
    end
    assert_equal(0, d.emits.size)
  end
end