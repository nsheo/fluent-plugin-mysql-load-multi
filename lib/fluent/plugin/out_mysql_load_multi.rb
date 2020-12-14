module Fluent
  class MysqlLoadMultiOutput < Fluent::Output
    Fluent::Plugin.register_output('mysql_load_multi', self)

    helpers :compat_parameters, :inject

    QUERY_TEMPLATE = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s (%s)"

    # Define `log` method for v0.10.42 or earlier
    unless method_defined?(:log)
      define_method("log") { $log }
    end

    def initialize
      require 'mysql2'
      require 'tempfile'
      super
    end

    config_param :host, :string, :default => 'localhost', desc: "Database host."
    config_param :port, :integer, :default => 3306, desc: "Database port."
    config_param :username, :string, :default => 'root', desc: "Database name."
    config_param :password, :string, :default => nil, desc: "Database user."
    config_param :database, :string, :default => nil, desc: "Database password."
    config_param :tablename, :string, :default => nil, desc: "Bulk insert table."
    config_param :key_names, :string, :default => nil, desc: "fleuntd target key, time can be override ${time}" 
    config_param :column_names, :string, :default => nil, desc: "Load insert column."
    config_param :encoding, :string, :default => 'utf8', desc: "Encoding option."
    config_param :sslkey, :string, default: nil, desc: "SSL key."
    config_param :sslcert, :string, default: nil, desc: "SSL cert."
    config_param :sslca, :string, default: nil, desc: "SSL CA."
    config_param :sslcapath, :string, default: nil, desc: "SSL CA path."
    config_param :sslcipher, :string, default: nil, desc: "SSL cipher."
    config_param :sslverify, :bool, default: nil, desc: "SSL Verify Server Certificate."
    config_param :transaction_isolation_level, :enum, list: [:read_uncommitted, :read_committed, :repeatable_read, :serializable], default: nil,
                 desc: "Set transaction isolation level."

    def configure(conf)
      compat_parameters_convert(conf, :buffer, :inject)
      super
      if @database.nil? || @tablename.nil? || @column_names.nil?
        raise Fluent::ConfigError, "database and tablename and column_names is required."
      end

      @key_names = @key_names.nil? ? @column_names.split(',') : @key_names.split(',')
      unless @column_names.split(',').count == @key_names.count
        raise Fluent::ConfigError, "It does not take the integrity of the key_names and column_names."
      end
    end

    def start
      super
    end

    def shutdown
      super
    end

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def formatted_to_msgpack_binary
      true
    end

    def multi_workers_ready?
      true
    end

    def check_table_schema(table: @tablename)
      _client = get_connection
      result = _client.xquery("SHOW COLUMNS FROM #{table}")
      max_lengths = []
      column_names_arr = @column_names.split(',')
      column_names_arr.each do |column|
        info = result.select { |x| x['Field'] == column }.first
        r = /(char|varchar)\(([\d]+)\)/
        begin
          max_length = info['Type'].scan(r)[0][1].to_i
        rescue
          max_length = nil
        end
        max_lengths << max_length
      end
      max_lengths
    ensure
      if not _client.nil? then _client.close end
    end

    def expand_placeholders(metadata)
      database = extract_placeholders(@database, metadata).gsub('.', '_')
      table = extract_placeholders(@tablename, metadata).gsub('.', '_')
      return database, table
    end

    def write(chunk)
      database, tablename = expand_placeholders(chunk.metadata)
      max_lengths = check_table_schema(table: tablename)
      data_count = 0
      tmp = Tempfile.new("mysql-loaddata-multi")
      chunk.msgpack_each do |tag, time, data|
        tmp.write format_proc.call(tag, time, data, max_lengths).join("\t") + "\n"
        data_count += 1
      end
      tmp.close

      conn = get_connection
      conn.query("SET SESSION TRANSACTION ISOLATION LEVEL #{transaction_isolation_level}") if @transaction_isolation_level
      conn.query(QUERY_TEMPLATE % ([tmp.path, @tablename, @column_names]))
      conn.close

      log.info "number that is registered in the \"%s:%s\" table is %d" % ([@database, @tablename, data_count])
    end

    private
    
    def format_proc
      proc do |tag, time, record, max_lengths|
        values = []
        @key_names.each_with_index do |key, i|
          if key == '${time}'
            value = Time.at(time).strftime('%Y-%m-%d %H:%M:%S')
          else
            if max_lengths[i].nil? || record[key].nil?
              value = record[key]
            else
              value = record[key].to_s.slice(0, max_lengths[i])
            end
          end
          values << value
        end
        values
      end
    end

    def get_connection
        Mysql2::Client.new({
            :host => @host,
            :port => @port,
            :username => @username,
            :password => @password,
            :database => @database,
            :encoding => @encoding,
            :sslkey => @sslkey,
            :sslcert => @sslcert,
            :sslca => @sslca,
            :sslcapath => @sslcapath,
            :sslcipher => @sslcipher,
            :sslverify => @sslverify,
            :local_infile => true,
            :flags => Mysql2::Client::MULTI_STATEMENTS
          })
    end
  
    def transaction_isolation_level
      case @transaction_isolation_level
      when :read_uncommitted
        "READ UNCOMMITTED"
      when :read_committed
        "READ COMMITTED"
      when :repeatable_read
        "REPEATABLE READ"
      when :serializable
        "SERIALIZABLE"
      end
    end
  end
end
