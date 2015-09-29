module Poseidon::OffsetManager

  class KafkaManager
    attr_reader :connection, :group_name, :topic
    def initialize(options = {})
      conn = options.fetch(:connection)
      @group_name = options.fetch(:group_name)
      @topic = options.fetch(:topic)

      res = conn.get_consumer_metadata(@group_name)
      raise "Cannot read from offsets topic" unless res.error == 0

      @connection = Poseidon::Connection.new(
        res.coordinator_host,
        res.coordinator_port,
        "#{@group_name}_#{@topic}",
        200,
        200
      )
    end

    def set(partition, offset)
      connection.set_consumer_offset(group_name, {
        topic => [[partition, offset, '']]
      })
    end

    def get(partition)
      res = connection.fetch_consumer_offset(group_name, {
        topic => [partition]
      })
      partition = res.topic_responses.first.partitions.first
      if partition.error == 0
        partition.offset
      else
        puts "WARNING: got error: #{Poseidon::Errors::ERROR_CODES[partition.error].name}"
        0
      end
    end
  end

  class ZookeeperManager
    attr_reader :zk, :group_name, :topic
    def initialize(options = {})
      @zk = options.fetch(:zk)
      @group_name = options.fetch(:group_name)
      @topic = options.fetch(:topic)
    end

    def set(partition, offset)
      zk.set offset_path(partition), offset.to_s
    rescue ZK::Exceptions::NoNode
      zk.create offset_path(partition), offset.to_s, ignore: :node_exists
    end

    def get(partition)
      data, _ = zk.get offset_path(partition), ignore: :no_node
      data.to_i
    end

    protected

    # @return [String] zookeeper offset storage path
    def offset_path(partition)
      "/consumers/#{group_name}/offsets/#{topic}/#{partition}"
    end
  end

end