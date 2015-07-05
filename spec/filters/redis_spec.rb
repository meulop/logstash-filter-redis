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

  describe "Ignores irrelevant tag" do
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
      # Did the Redis value get set correctly?
      @stored = @redis.get("logstash-filter-redis-test")
      insist { JSON.parse(@stored)["message"] } == "Storing message"
    end
  end

  describe "Retrieves the stored data" do
    config <<-CONFIG
    filter{
      redis {
        store_tag => "BEGIN"
	retrieve_tag => "END"
	key => "logstash-filter-redis-test"
      }
    }
    CONFIG

    eventstore = {
      "message" => "Storing message",
      "tags"    => ["BEGIN"]
    }
    eventretrieve = {
      "message" => "Retrieving message",
      "tags"    => ["END"]
    }

    sample([eventstore, eventretrieve]) do
      insist{ subject[1]["old_message"] } == "Storing message"
    end
  end

  describe "Handles overlapping sets of events" do
    # Fill me in

    config <<-CONFIG
    filter {
      redis {
        store_tag => "BEGIN"
	retrieve_tag => "END"
	key => "logstash-filter-redis-test-%{message}"
      }
    }
    CONFIG

    eventsstore = []
    eventsretrieve = []
    5.times do |i|
     eventsstore << {
       "message" => i.to_s,
       "tags"    => ["BEGIN"]
     }
     eventsretrieve << {
       "message" => i.to_s,
       "tags"    => ["END"]
     }
    end
    eventsretrieve.shuffle!
    sample(eventsstore + eventsretrieve) do
      insist {
        subject[5..9].select { |e| e["old_message"] == e["message"] }
      } != []
    end
  end


  describe "Deletes after retrieval" do
    config <<-CONFIG
      filter {
        redis {
	  store_tag      => "BEGIN"
	  retrieve_tag   => "END"
	  delete         => true
	  key            => "logstash-filter-redis-test"
	}
      }
    CONFIG
    estore = { "message" => "Store", "tags" => ["BEGIN"] }
    ereceive1 = { "message" => "Receive1", "tags" => ["END"] }
    ereceive2 = { "message" => "Receive2", "tags" => ["END"] }

    sample([estore,ereceive1,ereceive2]) do
      insist { subject[1]["old_message"] } == "Store"
      insist { subject[2]["old_message"] } == nil
    end
  end
end
