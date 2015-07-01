require 'spec_helper'
require "logstash/filters/redis"
require "redis"
require "pp"
require "json"

describe LogStash::Filters::Redis do

  after(:each) do
    @redis = Redis.new()
    @redis.del("logstash-filter-redis-test*")
  end

  describe "Stores key and values" do
    config <<-CONFIG
      filter {
        redis {
          store_tag => "BEGIN"
	  retrieve_tag => "END"
	  key => "logstash-filter-redis-test"
        }
      }
    CONFIG

    sample({"message" => "Storing message", "tags" => ["BEGIN"]}) do
      @redis = Redis.new()
      @stored = @redis.get("logstash-filter-redis-test")
      insist { subject["tags"].include?("STORED") } == true
      insist { subject["tags"].include?("BUGGERED") } == false
      insist { JSON.parse(@stored)["message"] } == "Storing message"
    end
  end
end
