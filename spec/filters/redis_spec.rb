require 'spec_helper'
require "logstash/filters/redis"
require "redis"
require "pp"
require "json"

describe LogStash::Filters::Redis do
  before(:all) do
    @redis = Redis.new()
  end

  after(:each) do
    sleep(0.2)
    @redis.keys("logstash-filter-redis-test*").each do |k|
      @redis.del(k)
    end
  end

  describe "Ignores irrelevant key" do
    config <<-CONFIG
      filter {
        redis {
          add_tag => "STORED"
          store_tag => "BEGIN"
          retrieve_tag => "END"
          key => "logstash-filter-redis-test"
        }
      }
    CONFIG

    sample({"message" => "Test message", "tags" => ["APACHE"]}) do
      insist { subject["tags"].include?("STORED") } == false
      insist { @redis.get("logstash-filter-redis-test") } == nil
    end

  end

  describe "Stores key and values" do
    config <<-CONFIG
      filter {
        redis {
          add_tag => "STORED"
          store_tag => "BEGIN"
	  retrieve_tag => "END"
	  key => "logstash-filter-redis-test"
        }
      }
    CONFIG

    sample({"message" => "Storing message", "tags" => ["BEGIN"]}) do
      # Did we add tag to the event?
      insist { subject["tags"].include?("STORED") } == true
      # Did the Redis value get set correctly
      @stored = @redis.get("logstash-filter-redis-test")
      insist { JSON.parse(@stored)["message"] } == "Storing message"
    end
  end
end
