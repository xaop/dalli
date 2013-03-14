require 'socket'
require 'timeout'

module Dalli
  class Server
    attr_accessor :hostname
    attr_accessor :port
    attr_accessor :weight
    attr_accessor :options
    attr_reader :sock

    DEFAULTS = {
      # seconds between trying to contact a remote server
      :down_retry_delay => 1,
      # connect/read/write timeout for socket operations
      :socket_timeout => 0.5,
      # times a socket operation may fail before considering the server dead
      :socket_max_failures => 2,
      # amount of time to sleep between retries when a failure occurs
      :socket_failure_delay => 0.01,
      # max size of value in bytes (default is 1 MB, can be overriden with "memcached -I <size>")
      :value_max_bytes => 1024 * 1024,
      :compressor => Compressor,
      # min byte size to attempt compression
      :compression_min_size => 1024,
      # max byte size for compression
      :compression_max_size => false,
      :serializer => Marshal,
      :username => nil,
      :password => nil,
      :keepalive => true
    }

    def initialize(attribs, options = {})
      (@hostname, @port, @weight) = attribs.split(':')
      @port ||= 11211
      @port = Integer(@port)
      @weight ||= 1
      @weight = Integer(@weight)
      @fail_count = 0
      @down_at = nil
      @last_down_at = nil
      @options = DEFAULTS.merge(options)
      @sock = nil
      @msg = nil
      @pid = nil
      @inprogress = nil
      @expected_responses = {}
      @expected_responses_by_thread = {}
      @gathered_responses = {}
    end

    # Chokepoint method for instrumentation
    def request(op, *args)
      verify_state
      raise Dalli::NetworkError, "#{hostname}:#{port} is down: #{@error} #{@msg}" unless alive?
      begin
        send(op, *args)
      rescue Dalli::NetworkError
        raise
      rescue Dalli::MarshalError => ex
        Dalli.logger.error "Marshalling error for key '#{args.first}': #{ex.message}"
        Dalli.logger.error "You are trying to cache a Ruby object which cannot be serialized to memcached."
        Dalli.logger.error ex.backtrace.join("\n\t")
        false
      rescue Dalli::DalliError
        raise
      rescue => ex
        Dalli.logger.error "Unexpected exception in Dalli: #{ex.class.name}: #{ex.message}"
        Dalli.logger.error "This is a bug in Dalli, please enter an issue in Github if it does not already exist."
        Dalli.logger.error ex.backtrace.join("\n\t")
        down!
      end
    end

    def alive?
      return true if @sock

      if @last_down_at && @last_down_at + options[:down_retry_delay] >= Time.now
        time = @last_down_at + options[:down_retry_delay] - Time.now
        Dalli.logger.debug { "down_retry_delay not reached for #{hostname}:#{port} (%.3f seconds left)" % time }
        return false
      end

      connect
      !!@sock
    rescue Dalli::NetworkError
      false
    end

    def close
      return unless @sock
      @sock.close rescue nil
      @sock = nil
      @pid = nil
      @inprogress = false
    end

    def lock!
    end

    def unlock!
    end

    def serializer
      @options[:serializer]
    end

    def compressor
      @options[:compressor]
    end

    # Start reading key/value pairs from this connection. This is usually called
    # after a series of GETKQ commands. A NOOP is sent, and the server begins
    # flushing responses for kv pairs that were found.
    #
    # Returns nothing.
    def multi_response_start
      verify_state
      write_noop
      @multi_buffer = ''
      @position = 0
      @inprogress = true
    end

    # Did the last call to #multi_response_start complete successfully?
    def multi_response_completed?
      @multi_buffer.nil?
    end

    # Attempt to receive and parse as many key/value pairs as possible
    # from this server. After #multi_response_start, this should be invoked
    # repeatedly whenever this server's socket is readable until
    # #multi_response_completed?.
    #
    # Returns a Hash of kv pairs received.
    def multi_response_nonblock
      raise 'multi_response has completed' if @multi_buffer.nil?

      @multi_buffer << @sock.read_available
      buf = @multi_buffer
      pos = @position
      values = {}

      while buf.bytesize - pos >= 24
        header = buf.slice(pos, 24)
        (key_length, _, body_length, _) = header.unpack(KV_HEADER)

        if key_length == 0
          # all done!
          @multi_buffer = nil
          @position = nil
          @inprogress = false
          break

        elsif buf.bytesize - pos >= 24 + body_length
          flags = buf.slice(pos + 24, 4).unpack('N')[0]
          key = buf.slice(pos + 24 + 4, key_length)
          value = buf.slice(pos + 24 + 4 + key_length, body_length - key_length - 4) if body_length - key_length - 4 > 0

          pos = pos + 24 + body_length

          begin
            values[key] = deserialize(value, flags)
          rescue DalliError
          end

        else
          # not enough data yet, wait for more
          break
        end
      end
      @position = pos

      values
    rescue SystemCallError, Timeout::Error, EOFError
      failure!
    end

    def receive_expected_responses
      responses = []
      if this_thread =  @expected_responses_by_thread[Thread.current]
        if (this_thread - @gathered_responses.keys).any?
          request_id = generate_opaque
          write_noop(request_id)
          # This forces reading in all expected responses in the gathered_responses
          generic_response(request_id)
        end

        this_thread.each do |opaque|
          response = @gathered_responses.delete(opaque)
          if response
            responses << response
          else
            @expected_responses.delete(opaque)
          end
        end
      end
      responses
    rescue SystemCallError, Timeout::Error, EOFError
      failure!
    ensure
      @expected_responses_by_thread[Thread.current] = nil
    end


    # Abort an earlier #multi_response_start. Used to signal an external
    # timeout. The underlying socket is disconnected, and the exception is
    # swallowed.
    #
    # Returns nothing.
    def multi_response_abort
      @multi_buffer = nil
      @position = nil
      @inprogress = false
      failure!
    rescue NetworkError
      true
    end

    # NOTE: Additional public methods should be overridden in Dalli::Threadsafe

    private

    def verify_state
      failure! if @inprogress
      failure! if @pid && @pid != Process.pid
    end

    def failure!
      Dalli.logger.info { "#{hostname}:#{port} failed (count: #{@fail_count})" }

      @expected_responses.keys.each do |k|
        @gathered_responses[k] = {:error_type => ::Dalli::NetworkError, :message => "failure occurred"}
      end
      @expected_responses = {}

      @fail_count += 1
      if @fail_count >= options[:socket_max_failures]
        down!
      else
        close
        sleep(options[:socket_failure_delay]) if options[:socket_failure_delay]
        raise Dalli::NetworkError, "Socket operation failed, retrying..."
      end
    end

    def down!
      close

      @last_down_at = Time.now

      if @down_at
        time = Time.now - @down_at
        Dalli.logger.debug { "#{hostname}:#{port} is still down (for %.3f seconds now)" % time }
      else
        @down_at = @last_down_at
        Dalli.logger.warn { "#{hostname}:#{port} is down" }
      end

      @error = $! && $!.class.name
      @msg = @msg || ($! && $!.message && !$!.message.empty? && $!.message)
      raise Dalli::NetworkError, "#{hostname}:#{port} is down: #{@error} #{@msg}"
    end

    def up!
      if @down_at
        time = Time.now - @down_at
        Dalli.logger.warn { "#{hostname}:#{port} is back (downtime was %.3f seconds)" % time }
      end

      @fail_count = 0
      @down_at = nil
      @last_down_at = nil
      @msg = nil
      @error = nil
    end

    def multi?
      Thread.current[:dalli_multi]
    end

    def get(key)
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:get], key.bytesize, 0, 0, 0, key.bytesize, request_id, 0, key].pack(FORMAT[:get])
      write(req)
      generic_response(request_id, true)
    end

    def getkq(key)
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:getkq], key.bytesize, 0, 0, 0, key.bytesize, request_id, 0, key].pack(FORMAT[:getkq])
      write(req)
    end

    def set(key, value, ttl, cas, options)
      (value, flags) = serialize(key, value, options)

      if under_max_value_size?(value)
        request_id = generate_opaque
        req = [REQUEST, OPCODES[multi? ? :setq : :set], key.bytesize, 8, 0, 0, value.bytesize + key.bytesize + 8, request_id, cas, flags, ttl, key, value].pack(FORMAT[:set])
        write(req)
        if multi?
          expect_response(request_id)
        else
          generic_response(request_id)
        end
      else
        false
      end
    end

    def add(key, value, ttl, options)
      (value, flags) = serialize(key, value, options)

      if under_max_value_size?(value)
        request_id = generate_opaque
        req = [REQUEST, OPCODES[multi? ? :addq : :add], key.bytesize, 8, 0, 0, value.bytesize + key.bytesize + 8, request_id, 0, flags, ttl, key, value].pack(FORMAT[:add])
        write(req)
        if multi?
          expect_response(request_id)
        else
          generic_response(request_id)
        end
      else
        false
      end
    end

    def replace(key, value, ttl, options)
      (value, flags) = serialize(key, value, options)

      if under_max_value_size?(value)
        request_id = generate_opaque
        req = [REQUEST, OPCODES[multi? ? :replaceq : :replace], key.bytesize, 8, 0, 0, value.bytesize + key.bytesize + 8, request_id, 0, flags, ttl, key, value].pack(FORMAT[:replace])
        write(req)
        if multi?
          expect_response(request_id)
        else
          generic_response(request_id)
        end
      else
        false
      end
    end

    def delete(key)
      request_id = generate_opaque
      req = [REQUEST, OPCODES[multi? ? :deleteq : :delete], key.bytesize, 0, 0, 0, key.bytesize, request_id, 0, key].pack(FORMAT[:delete])
      write(req)
      if multi?
        expect_response(request_id)
      else
        generic_response(request_id)
      end
    end

    def flush(ttl)
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:flush], 0, 4, 0, 0, 4, request_id, 0, 0].pack(FORMAT[:flush])
      write(req)
      generic_response(request_id)
    end

    def decr(key, count, ttl, default)
      request_id = generate_opaque
      expiry = default ? ttl : 0xFFFFFFFF
      default ||= 0
      (h, l) = split(count)
      (dh, dl) = split(default)
      req = [REQUEST, OPCODES[:decr], key.bytesize, 20, 0, 0, key.bytesize + 20, request_id, 0, h, l, dh, dl, expiry, key].pack(FORMAT[:decr])
      write(req)
      generic_response(request_id)
    end

    def incr(key, count, ttl, default)
      request_id = generate_opaque
      expiry = default ? ttl : 0xFFFFFFFF
      default ||= 0
      (h, l) = split(count)
      (dh, dl) = split(default)
      req = [REQUEST, OPCODES[:incr], key.bytesize, 20, 0, 0, key.bytesize + 20, request_id, 0, h, l, dh, dl, expiry, key].pack(FORMAT[:incr])
      write(req)
      generic_response(request_id)
    end

    def write_noop(request_id = 0)
      # No request_id needed: since there is no response anyway
      req = [REQUEST, OPCODES[:noop], 0, 0, 0, 0, 0, request_id, 0].pack(FORMAT[:noop])
      write(req)
    end

    def append(key, value)
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:append], key.bytesize, 0, 0, 0, value.bytesize + key.bytesize, request_id, 0, key, value].pack(FORMAT[:append])
      write(req)
      generic_response(request_id)
    end

    def prepend(key, value)
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:prepend], key.bytesize, 0, 0, 0, value.bytesize + key.bytesize, request_id, 0, key, value].pack(FORMAT[:prepend])
      write(req)
      generic_response(request_id)
    end

    def stats(info='')
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:stat], info.bytesize, 0, 0, 0, info.bytesize, request_id, 0, info].pack(FORMAT[:stat])
      write(req)
      stats_response(request_id)
    end

    def reset_stats
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:stat], 'reset'.bytesize, 0, 0, 0, 'reset'.bytesize, request_id, 0, 'reset'].pack(FORMAT[:stat])
      write(req)
      generic_response(request_id)
    end

    def cas(key)
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:get], key.bytesize, 0, 0, 0, key.bytesize, request_id, 0, key].pack(FORMAT[:get])
      write(req)
      cas_response(request_id)
    end

    def version
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:version], 0, 0, 0, 0, 0, request_id, 0].pack(FORMAT[:noop])
      write(req)
      generic_response(request_id)
    end

    def touch(key, ttl)
      request_id = generate_opaque
      req = [REQUEST, OPCODES[:touch], key.bytesize, 4, 0, 0, key.bytesize + 4, request_id, 0, ttl, key].pack(FORMAT[:touch])
      write(req)
      generic_response(request_id)
    end

    # http://www.hjp.at/zettel/m/memcached_flags.rxml
    # Looks like most clients use bit 0 to indicate native language serialization
    # and bit 1 to indicate gzip compression.
    FLAG_SERIALIZED = 0x1
    FLAG_COMPRESSED = 0x2

    def serialize(key, value, options=nil)
      marshalled = false
      value = unless options && options[:raw]
        marshalled = true
        begin
          self.serializer.dump(value)
        rescue => ex
          # Marshalling can throw several different types of generic Ruby exceptions.
          # Convert to a specific exception so we can special case it higher up the stack.
          exc = Dalli::MarshalError.new(ex.message)
          exc.set_backtrace ex.backtrace
          raise exc
        end
      else
        value.to_s
      end
      compressed = false
      if @options[:compress] && value.bytesize >= @options[:compression_min_size] &&
        (!@options[:compression_max_size] || value.bytesize <= @options[:compression_max_size])
        value = self.compressor.compress(value)
        compressed = true
      end

      flags = 0
      flags |= FLAG_COMPRESSED if compressed
      flags |= FLAG_SERIALIZED if marshalled
      [value, flags]
    end

    def deserialize(value, flags)
      value = self.compressor.decompress(value) if (flags & FLAG_COMPRESSED) != 0
      value = self.serializer.load(value) if (flags & FLAG_SERIALIZED) != 0
      value
    rescue TypeError
      raise if $!.message !~ /needs to have method `_load'|exception class\/object expected|instance of IO needed|incompatible marshal file format/
      raise UnmarshalError, "Unable to unmarshal value: #{$!.message}"
    rescue ArgumentError
      raise if $!.message !~ /undefined class|marshal data too short/
      raise UnmarshalError, "Unable to unmarshal value: #{$!.message}"
    rescue Zlib::Error
      raise UnmarshalError, "Unable to uncompress value: #{$!.message}"
    end

    def cas_response(request_id)
      header = read(24)
      raise Dalli::NetworkError, 'No response' if !header
      response(request_id, header, true)
    end

    NORMAL_HEADER = '@4CCnNN'
    KV_HEADER = '@2n@6nNN'

    FULL_HEADER = 'CCnCCnNNQ'

    def under_max_value_size?(value)
      value.bytesize <= @options[:value_max_bytes]
    end

    def response(request_id, header, unpack = false, raise_errors = true)
      _magic, opcode, key_length, extras, _data_type, status, body_length, opaque, cas = header.unpack(FULL_HEADER)
      while request_id != opaque
        if @expected_responses.has_key?(opaque)
          expected_response = response(opaque, header, @expected_responses.delete(opaque), false)
          @gathered_responses[opaque] = expected_response
        else
          raise ::Dalli::DalliError, "Unexpected response with opaque #{opaque} instead of expected #{request_id}"
        end
        header = read(24)
        _magic, opcode, key_length, extras, _data_type, status, body_length, opaque, cas = header.unpack(FULL_HEADER)
      end
      if body_length > 0
        data = read(body_length)

        flags = data.slice(0, extras).unpack('N')[0]
        key = data.slice(extras, key_length)
        value = data.slice(extras + key_length, body_length - key_length - extras) if body_length - key_length - extras > 0
      end

      case INVERSE_OPCODES[opcode]
      when :get, :set, :add, :replace, :delete, :version, :flush, :append, :prepend
        if status == 1
          nil
        elsif status == 2 || status == 5
          false # Not stored, normal status for add operation
        elsif status != 0
          custom_raise ::Dalli::DalliError, "Response error #{status}: #{RESPONSE_CODES[status]}", raise_errors
        elsif data
          value = unpack ? deserialize(value, flags) : value
          [value, cas]
        else
          true
        end
      when :touch
        if status == 1
          nil
        elsif status != 0
          custom_raise ::Dalli::DalliError, "Response error #{status}: #{RESPONSE_CODES[status]}", raise_errors
        else
          true
        end
      when :getkq
        # Quiet get will not respond with a not found message
        if status == 1
          custom_raise ::Dalli::DalliError, "Unexpected not found response for quiet get", raise_errors
        elsif status != 0
          custom_raise ::Dalli::DalliError, "Response error #{status}: #{RESPONSE_CODES[status]}", raise_errors
        else
          unpack ? deserialize(value, flags) : value
        end
      when :setq, :addq, :replaceq, :deleteq
        # Quiet mutations only respond in the case of a failure
        if status == 1 # Key not found
          nil
        elsif status != 0
          custom_raise ::Dalli::DalliError, "Response error #{status}: #{RESPONSE_CODES[status]}", raise_errors
        else
          custom_raise ::Dalli::DalliError, "Unexpected success response for quiet mutation", raise_errors
        end
      when :incr, :decr
        if status == 1 # Key not found
          nil
        elsif status != 0
          custom_raise Dalli::DalliError, "Response error #{status}: #{RESPONSE_CODES[status]}", raise_errors
        elsif status == 0
          value ? longlong(*value.unpack('NN')) : value
        end
      when :noop
        # Do nothing?
      when :stat
        if status == 0 && data
          [true, key, value]
        elsif status == 0
          true
        else
          custom_raise Dalli::DalliError, "Response error #{status}: #{RESPONSE_CODES[status]}", raise_errors
        end
      when :incrq, :decrq
        # Reserverved opcodes, no way to handle this (and it shouldn't occur anyway)
        raise DalliError, "Reserved opcode encountered while parsing response: 0x#{opcode.to_s(16)}"
      else
        raise DalliError, "Unknown opcode encountered while parsing response: 0x#{opcode.to_s(16)}"
      end
    end

    def generic_response(request_id, unpack=false)
      header = read(24)
      raise Dalli::NetworkError, 'No response' if !header
      value, _ = response(request_id, header, unpack)
      value
    end

    def stats_response(request_id)
      hash = {}
      loop do
        header = read(24)
        raise Dalli::NetworkError, 'No response' if !header
        _, key, value = response(request_id, header)
        return hash if key.nil?
        hash[key] = value
      end
    end

    def write(bytes)
      begin
        @inprogress = true
        result = @sock.write(bytes)
        @inprogress = false
        result
      rescue SystemCallError, Timeout::Error
        failure!
      end
    end

    def read(count)
      begin
        @inprogress = true
        data = @sock.readfull(count)
        @inprogress = false
        data
      rescue SystemCallError, Timeout::Error, EOFError
        failure!
      end
    end

    def connect
      Dalli.logger.debug { "Dalli::Server#connect #{hostname}:#{port}" }

      begin
        @pid = Process.pid
        @sock = KSocket.open(hostname, port, self, options)
        @version = version # trigger actual connect
        sasl_authentication if need_auth?
        up!
      rescue Dalli::DalliError # SASL auth failure
        raise
      rescue SystemCallError, Timeout::Error, EOFError, SocketError
        # SocketError = DNS resolution failure
        failure!
      end
    end

    def split(n)
      [n >> 32, 0xFFFFFFFF & n]
    end

    def longlong(a, b)
      (a << 32) | b
    end

    REQUEST = 0x80
    RESPONSE = 0x81

    RESPONSE_CODES = {
      0 => 'No error',
      1 => 'Key not found',
      2 => 'Key exists',
      3 => 'Value too large',
      4 => 'Invalid arguments',
      5 => 'Item not stored',
      6 => 'Incr/decr on a non-numeric value',
      0x20 => 'Authentication required',
      0x81 => 'Unknown command',
      0x82 => 'Out of memory',
    }

    OPCODES = {
      :get => 0x00,
      :set => 0x01,
      :add => 0x02,
      :replace => 0x03,
      :delete => 0x04,
      :incr => 0x05,
      :decr => 0x06,
      :flush => 0x08,
      :noop => 0x0A,
      :version => 0x0B,
      :getkq => 0x0D,
      :append => 0x0E,
      :prepend => 0x0F,
      :stat => 0x10,
      :setq => 0x11,
      :addq => 0x12,
      :replaceq => 0x13,
      :deleteq => 0x14,
      :incrq => 0x15,
      :decrq => 0x16,
      :auth_negotiation => 0x20,
      :auth_request => 0x21,
      :auth_continue => 0x22,
      :touch => 0x1C,
    }

    INVERSE_OPCODES = OPCODES.inject({}) {|h,(k,v)| h[v] = k; h }

    HEADER = "CCnCCnNNQ"
    OP_FORMAT = {
      :get => 'a*',
      :set => 'NNa*a*',
      :add => 'NNa*a*',
      :replace => 'NNa*a*',
      :delete => 'a*',
      :incr => 'NNNNNa*',
      :decr => 'NNNNNa*',
      :flush => 'N',
      :noop => '',
      :getkq => 'a*',
      :version => '',
      :stat => 'a*',
      :append => 'a*a*',
      :prepend => 'a*a*',
      :auth_request => 'a*a*',
      :auth_continue => 'a*a*',
      :touch => 'Na*',
    }
    FORMAT = OP_FORMAT.inject({}) { |memo, (k, v)| memo[k] = HEADER + v; memo }


    #######
    # SASL authentication support for NorthScale
    #######

    def need_auth?
      @options[:username] || ENV['MEMCACHE_USERNAME']
    end

    def username
      @options[:username] || ENV['MEMCACHE_USERNAME']
    end

    def password
      @options[:password] || ENV['MEMCACHE_PASSWORD']
    end

    def sasl_authentication
      Dalli.logger.info { "Dalli/SASL authenticating as #{username}" }

      # negotiate
      req = [REQUEST, OPCODES[:auth_negotiation], 0, 0, 0, 0, 0, 0, 0].pack(FORMAT[:noop])
      write(req)
      header = read(24)
      raise Dalli::NetworkError, 'No response' if !header
      (extras, type, status, count, _) = header.unpack(NORMAL_HEADER)
      raise Dalli::NetworkError, "Unexpected message format: #{extras} #{count}" unless extras == 0 && count > 0
      content = read(count)
      return (Dalli.logger.debug("Authentication not required/supported by server")) if status == 0x81
      mechanisms = content.split(' ')
      raise NotImplementedError, "Dalli only supports the PLAIN authentication mechanism" if !mechanisms.include?('PLAIN')

      # request
      mechanism = 'PLAIN'
      msg = "\x0#{username}\x0#{password}"
      req = [REQUEST, OPCODES[:auth_request], mechanism.bytesize, 0, 0, 0, mechanism.bytesize + msg.bytesize, 0, 0, mechanism, msg].pack(FORMAT[:auth_request])
      write(req)

      header = read(24)
      raise Dalli::NetworkError, 'No response' if !header
      (extras, type, status, count, _) = header.unpack(NORMAL_HEADER)
      raise Dalli::NetworkError, "Unexpected message format: #{extras} #{count}" unless extras == 0 && count > 0
      content = read(count)
      return Dalli.logger.info("Dalli/SASL: #{content}") if status == 0

      raise Dalli::DalliError, "Error authenticating: #{status}" unless status == 0x21
      raise NotImplementedError, "No two-step authentication mechanisms supported"
      # (step, msg) = sasl.receive('challenge', content)
      # raise Dalli::NetworkError, "Authentication failed" if sasl.failed? || step != 'response'
    end

    def generate_opaque
      @opaque ||= -1
      @opaque = @opaque + 1
    end

    def expect_response(request_id, unpack = false)
      @expected_responses[request_id] = unpack
      @expected_responses_by_thread[Thread.current] ||= []
      @expected_responses_by_thread[Thread.current] << request_id
    end

    def custom_raise(error, message, raise_errors)
      if raise_errors
        raise error, message
      else
        {:error_type => error, :message => message}
      end
    end
  end
end
