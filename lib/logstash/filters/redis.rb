# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"


class LogStash::Filters::Redis < LogStash::Filters::Base

  config_name "redis"

  # The hostname of your Redis server.
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect on.
  config :port, :validate => :number, :default => 6379

  # Password to authenticate with. There is no authentication by default.
  config :password, :validate => :password

  # The Redis database number.
  config :db, :validate => :number, :default => 0
  
  # Tag to store key
  config :store_tag, :validate => :string, :required => true

  # Tag to retrieve key
  config :retrieve_tag, :validate => :string, :required => true

  # Redis key name
  config :key, :validate => :string, :required => true

  # Delete on retrieval
  config :delete, :validate => :boolean, :default => false

  # Fields to store in the Redis value
  config :fields, :validate => :array, :default => ["message"]

  # Field prefix
  config :prefix, :validate => :string, :default => "old_"

  # Connection timeout
  config :timeout, :validate => :number, :required => false, :default => 5

  # Key expiry time
  config :expiry, :validate => :number, :default => 1800

  public
  def register
    require 'redis'
    require 'json'
    @redis = nil
    @redis_url = "redis://#{@password}@#{@host}:#{@port}/#{@db}"
  end # def register

  public
  def filter(event)
    return unless filter?(event)

    # Allow dynamic key names using fields etc
    key = event.sprintf(@key)

    relevant = [@store_tag, @retrieve_tag].select do |t|
       event["tags"].include?(t)
    end
 
    @logger.debug(relevant)
    return unless [] != relevant
 
    # Do we retrieve data from a prior event?
    # N.b. we retrieve before we store so that we can do both if we want!
    # (e.g. packet sequence numbers? ...)
    #
    if event["tags"].include?(@retrieve_tag)
      @logger.debug("Found retrieve tag %{retrieve_tag}")
      @redis ||= connect
      val = @redis.get(key)
      if val != nil
        @logger.debug("Found key in Redis")
        JSON.parse(val).each do |k,v|
          event[prefix + k] = v
        end
        if @delete
          @redis.del(key) && @logger.debug("Deleted key")
        end
      else
        @logger.debug("Key not found in Redis")
      end
    end
    # Do we store data? 
    if event["tags"].include?(@store_tag)
      @logger.debug("Found store tag %{store_tag}")
      val = event.to_hash().select { |name,value| fields.include?(name) }
      @redis ||= connect
      @redis.set(key, val.to_json) && @logger.debug("Stored key")
      @redis.expire(key, @expiry) && @logger.debug("Set expiry key")
    end
    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end # def filter

  private
  def connect
     Redis.new(
       :host => @host,
       :port => @port, 
       :timeout => @timeout,
       :db => db,
       :password => @password.nil? ? nil : @password.value
     )
  end #def connect
end # class LogStash::Filters::Redis
