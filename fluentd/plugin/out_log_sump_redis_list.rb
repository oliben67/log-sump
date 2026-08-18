# frozen_string_literal: true

require 'fluent/plugin/output'
require 'redis'
require 'json'

module Fluent
  module Plugin
    # Reproduces exactly what Logstash's `redis` output (`data_type =>
    # "list"`) + its `json`/`tag_on_failure` filter did in
    # logstash/pipeline/log-sump.conf: RPUSH the *original, untouched*
    # message string onto a Redis list, after validating it parses as JSON
    # and carries `kind`/`docker_host` -- dropping (not forwarding) anything
    # that doesn't. No Fluentd Redis-output gem reproduces this shape (list
    # RPUSH of a raw string, not a re-serialized/hash-mapped value; see the
    # migration note in ../README.md), hence a small custom plugin instead
    # of bending log-server's consumer.py (out of scope) to fit one.
    #
    # Deliberately validates *inside* write() (post-buffer) rather than in a
    # separate <filter> stage: a regex-based pre-filter can't reliably tell
    # valid from invalid JSON, and real JSON-parsing filter plugins would
    # just duplicate this same parse -- doing it once, here, keeps parity
    # with Logstash's pipeline (validate, then forward the untouched
    # original string, never a re-serialized copy) in one place.
    class LogSumpRedisListOutput < Output
      Fluent::Plugin.register_output('log_sump_redis_list', self)

      helpers :event_emitter

      desc 'Redis host'
      config_param :host, :string, default: '127.0.0.1'
      desc 'Redis port'
      config_param :port, :integer, default: 6379
      desc 'Redis DB index'
      config_param :db, :integer, default: 0
      desc 'Target Redis list key (RPUSH destination) -- must match log_sump_common.redis_keys.INGEST_LIST'
      config_param :key, :string
      desc 'Record field holding the raw, untouched message string (in_tcp\'s "none" parser default)'
      config_param :message_key, :string, default: 'message'

      config_section :buffer, multi: false do
        # flush_thread_count MUST stay 1: log-server's consumer (BLPOP/LPOP,
        # FIFO) and the shared logs+metrics interleaving both depend on
        # arrival order being preserved end to end -- concurrent flushes
        # could RPUSH two chunks' records out of arrival order. A single
        # flush thread also means a chunk that's retrying blocks the next
        # one rather than letting it jump ahead, so retries can't reorder
        # either.
        config_set_default :flush_thread_count, 1
        config_set_default :flush_mode, :interval
        config_set_default :flush_interval, 1
        config_set_default :retry_type, :exponential_backoff
        # Matches (does not weaken) today's failure semantics: the real
        # durability floor is log-listener's own on-disk
        # AsynchronousLogstashHandler buffer (out of scope, unchanged) --
        # this only needs to not lose anything Redis itself was briefly
        # unavailable for, same as Logstash's own output retry did.
        config_set_default :retry_forever, true
      end

      def start
        super
        @redis = ::Redis.new(host: @host, port: @port, db: @db)
      end

      def shutdown
        @redis&.close
        super
      end

      def formatted_to_msgpack_binary?
        true
      end

      def write(chunk)
        values = []
        chunk.each do |_time, record|
          raw = record[@message_key]
          next if raw.nil?

          parsed = begin
            JSON.parse(raw)
          rescue JSON::ParserError => e
            log.warn('log_sump_redis_list: dropping unparsable record', error: e.message)
            nil
          end
          next if parsed.nil?

          unless parsed.is_a?(Hash) && parsed.key?('kind') && parsed.key?('docker_host')
            log.warn('log_sump_redis_list: dropping schema-invalid record (missing kind/docker_host)')
            next
          end

          # The *original* string, not `parsed.to_json` -- a re-serialized
          # copy could reorder keys or reformat numbers differently than
          # what log-listener actually sent, exactly what Logstash's own
          # `codec => plain { format => "%{message}" }` avoided by writing
          # `message` back out untouched (see log-sump.conf's own docstring).
          values << raw
        end
        return if values.empty?

        # Pipelined, in order -- matches Logstash's own batched-RPUSH
        # behavior; still one RPUSH per record, just wire-batched.
        @redis.pipelined do |pipeline|
          values.each { |v| pipeline.rpush(@key, v) }
        end
      end
    end
  end
end
