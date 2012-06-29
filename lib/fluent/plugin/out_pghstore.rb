class Fluent::PgHStoreOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('pghstore', self)

  config_param :database, :string
  config_param :table, :string, :default => 'fluentd_store'
  config_param :host, :string, :default => 'localhost'
  config_param :port, :integer, :default => 5432
  config_param :user, :string, :default => nil
  config_param :password, :string, :default => nil

  config_param :table_option, :string, :default => nil

  config_param :key, :string

  def initialize
    super
    require 'pg'
  end

  def start
    super

    create_table(@table) unless table_exists?(@table)
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

    chunk.msgpack_each {|(tag, time_str, record)|
      sql = generate_sql(tag, time_str, record)
      begin
        conn.exec(sql)
      rescue PGError => e 
        $log.error "PGError: " + e.message  # dropped if error
      end
    }

    conn.close()
  end

  private

  def generate_sql(tag, time, record)
    target = record[@key]

    sql =<<"SQL"
increment ('#{@table}', '#{target}', '#{Time.at(time)}'::TIMESTAMP WITH TIME ZONE);
SQL

    return sql
  end

  def get_connection()
    if @conn != nil and @conn.finished?() == false
        return @conn  # connection is alived
    end

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
SELECT COUNT(*) FROM pg_tables WHERE tablename = '#{table}';
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
CREATE TABLE #{tablename} (
  value TEXT,
  time TIMESTAMP WITH TIME ZONE,
  count INT
);
CREATE FUNCTION increment(tablename TEXT, target_value TEXT, time_value TIMESTAMP WITH TIME ZONE) RETURNS VOID AS
$$
BEGIN
    -- first try to update the key
    UPDATE tablename SET count = count + 1 WHERE value = target_value AND time = time_value;
    IF found THEN
        RETURN;
    END IF;
    -- not there, so try to insert the key
    -- if someone else inserts the same key concurrently,
    -- we could get a unique-key failure
    INSERT INTO tablename(time,value,count) VALUES (time_value,target_value,1);
    RETURN;
END;
$$
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
