class Fluent::PgHStoreOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('pghstore', self)

  config_param :database, :string
  config_param :table, :string, :default => 'fluentd_store'
  config_param :host, :string, :default => 'localhost'
  config_param :port, :integer, :default => 5432
  config_param :user, :string, :default => nil
  config_param :password, :string, :default => nil

  config_param :table_option, :string, :default => nil
  
  config_param :time_slice_format, :string, :default => '.y%Y.m%m'
  config_param :remove_tag_prefix, :string, :default => 'action.'

  def initialize
    super
    require 'pg'
  end

  def start
    super

    if remove_tag_prefix = @remove_tag_prefix
      @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_tag_prefix))
    end

    #create_table(@table) unless table_exists?(@table)
  end

  def shutdown
    super

    if @conn != nil and @conn.finished?() == false
      conn.close()
    end
  end
  
  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    conn = get_connection()
    return if conn == nil  # TODO: chunk will be dropped. should retry?

    #insert the chunk
    chunk.msgpack_each {|(tag, time_str, record)|

      tag_array = tag.split(".", 4)
      table_name = tag_array[0]
      table_name << "."
      table_name << tag_array[1]
      time1 = Time.new
      time_str = time1.strftime(@time_slice_format)
      table_name << time_str
      table_name << "."
      table_name << tag_array[2]
      table_name = table_name.gsub(@remove_tag_prefix, '') if @remove_tag_prefix
      $log.warn "Table name: #{table_name}"
      
      table_name_attribute = table_name
      table_name_attribute << "Attribute"
      
      unless table_exists?(table_name) then
        create_table(table_name)
      end
      unless table_exists?(table_name_attribute) then
        create_table_attribute(table_name_attribute)
      end
      
      record['id'] = uuid(tag_array[1], time1)
      record['game_id'] = tag_array[1]
      
      conn = get_connection()
      return if conn == nil  # TODO: chunk will be dropped. should retry?

      sql = generate_sql(table_name, time_str, record)
      begin
        conn.exec(sql)
      rescue PGError => e 
        $log.error "PGError: " + e.message  # dropped if error
      end
      
      if record.has_key?('attributes')
        record['attributes'].each {|(key,value)|
          sql = generate_sql_attribute(table_name_attribute, time_str, record['id'], key, value)
          begin
            conn.exec(sql)
          rescue PGError => e 
            $log.error "PGError: " + e.message  # dropped if error
          end
        }
      end
    }

    conn.close()
  end
  
  def uuid(game_id, timestamp)
    a = game_id
    a << "-"
    a << timestamp.strftime("%Y-%m-%d")
    a << "-"
    a << SecureRandom.hex(12)
    $log.warn "Player action ID: #{a}"
    
    return a
  end

  private

  def generate_sql(table_name, time, record)
    k_list = []
    v_list = []
    kv_list = []
    record.each {|(key,value)|
      if key == "gameId"
        key = "game_id"
      end
      if key == "playerId"
        key = "fb_player_id"
      end
      if key == "virtualCurrency"
        key = "virtual_currency"
      end
      if key == "session_id"
        next
      elsif key == "sessionId"
        key = "session_id"
      end
      
      if key == "logAction"
        key = "log_action"
      end
      if key == "logaction"
        key = "log_action"
      end
      if key == "log_action"
        next unless value.is_a? Integer
      end
      
      if key == "successful"
        if value == "true"
          value = 1
        elsif value == "false"
          value = 0
        else
          value = 1 unless value.is_a? Integer
        end
      end
      
      if key == "requirements"
        next
      end
      if key == "rewards"
        next
      end
      if key == "action"
        next
      end
      if key == "_eventtype"
        next
      end
      if key == "attributes"
        next
      end
      
      if key == "logDatetime"
        key = "log_datetime"
        $log.warn "log_datetime = #{value}"
        begin value_i = Integer(value)
          #Timestamp is in UNIX timestamp format
          time2 = Time.at(value_i)
          value = time2.strftime("%Y-%m-%d %H:%M:%S.%6N")
          $log.warn "Integer timestamp - #{value}"
        rescue
          begin
            time2 = Date.strptime("%a, %d %b %Y %H:%M:%S %z")
            value = time2.strftime("%Y-%m-%d %H:%M:%S.%6N")
            $log.warn "String timestamp - #{value}"
          rescue
            next
          end
        end
      end
    
      k_list.push("#{key}")
      v_list.push("'#{value}'")
      kv_list.push("\"#{key}\" => \"#{value}\"")
    }
    
    k_list.push("timestamp")
    time1 = Time.at(Integer(time))
    time_str = time1.strftime("%Y-%m-%d %H:%M:%S.%6N")
    v_list.push("'#{time_str}'")

    sql =<<"SQL"
INSERT INTO \"#{table_name}\" (#{k_list.join(",")}) VALUES
(#{v_list.join(",")});
SQL

    return sql
  end

  def generate_sql_attribute(table_name, time, id, key, value)
  
    time1 = Time.at(time)
    time_str = time1.strftime("%Y-%m-%d %H:%M:%S.%6N")
    
    sql =<<"SQL"
INSERT INTO \"#{table_name}\" (player_action_id, key, value, created_datetime, updated_datetime) VALUES
(#{id}, #{key}, #{value}, #{time_str}, #{time_str});
SQL

    return sql
  end

  def get_connection()
    if @conn != nil and @conn.finished?() == false
        $log.warn "Connection is alive"
        return @conn  # connection is alived
    end

    $log.warn "Connection is NOT alive. Connecting"
    begin
      if @user
        @conn = PG.connect(:dbname => @database, :host => @host, :port => @port,
                           :user => @user, :password => @password)
      else
        @conn = PG.connect(:dbname => @database, :host => @host, :port => @port)
      end
    rescue PGError => e 
      $log.error "Error: could not connect database:" + @database
      return nil
    end
    return @conn

  end

  def table_exists?(table)
    sql =<<"SQL"
SELECT COUNT(*) FROM pg_tables WHERE LOWER(tablename) = LOWER('#{table}');
SQL
    conn = get_connection()
    raise "Could not connect the database at startup. abort." if conn == nil
    res = conn.exec(sql)
    conn.close
    if res[0]["count"] == "1"
      return true
    else
      return false
    end
  end

  def create_table(tablename)
    sql =<<"SQL"
CREATE TABLE "#{tablename}" (ID VARCHAR(64) NOT NULL ,
	 FB_PLAYER_ID VARCHAR(64),
	 GAME_ID BIGINT,
	 SESSION_ID VARCHAR(64),
	 TIMESTAMP TIMESTAMP,
	 LOG_ACTION SMALLINT,
	 TYPE VARCHAR(255),
	 DESCRIPTION VARCHAR(255),
	 SUCCESSFUL SMALLINT,
	 LEVEL INTEGER,
	 CREDIT INTEGER,
	 EXPERIENCE INTEGER,
	 ATTRIBUTES VARCHAR(1024),
	 VIRTUAL_CURRENCY VARCHAR(1024),
	 LOG_DATETIME TIMESTAMP,
	 LENGTH BIGINT,
 	 TIME BIGINT,
 	 H BIGINT,
	 PRIMARY KEY (ID));
SQL

    sql += @table_option if @table_option

    conn = get_connection()
    raise "Could not connect the database at create_table. abort." if conn == nil

    begin
      conn.exec(sql) 
    rescue PGError => e
      $log.error "Error at create_table:" + e.message
      $log.error "SQL:" + sql
    end
    conn.close

    $log.warn "table #{tablename} was not exist. created it."
  end

  def create_table_attribute(tablename)
    sql =<<"SQL"
CREATE TABLE "#{tablename}" (PLAYER_ACTION_ID VARCHAR(64) NOT NULL ,
	 KEY VARCHAR(128),
	 VALUE VARCHAR(128),
	 CREATED_DATETIME TIMESTAMP,
	 UPDATED_DATETIME TIMESTAMP,
	 PRIMARY KEY (PLAYER_ACTION_ID, KEY, VALUE));
SQL

    sql += @table_option if @table_option

    conn = get_connection()
    raise "Could not connect the database at create_table. abort." if conn == nil

    begin
      conn.exec(sql) 
    rescue PGError => e
      $log.error "Error at create_table:" + e.message
      $log.error "SQL:" + sql
    end
    conn.close

    $log.warn "table #{tablename} was not exist. created it."
  end

end
