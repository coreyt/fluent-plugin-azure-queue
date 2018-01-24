require "bundler/setup"
require "test/unit"
$LOAD_PATH.unshift(File.join(__dir__, "..", "lib"))
$LOAD_PATH.unshift(__dir__)
require "fluent/test"
require 'flexmock/test_unit'

Struct.new("QueueMessage", :id, :pop_receipt, :message_text)