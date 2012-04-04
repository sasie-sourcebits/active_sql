begin
  require 'activesql'
rescue LoadError
  if RUBY_PLATFORM =~ /mingw|mswin/ then
    RUBY_VERSION =~ /(\d+.\d+)/
    require "#{$1}/active_sql"
  end
end

module ActiveSQL
  class Base
     class << self 
 
       # Execute the sql queries.
       def select_sql(qry)
         if qry =~ my_select?    
           result = exec(qry, true)	
           cols = result.shift 
           result.instance_eval "def columns; #{cols.inspect}; end"
           result
         else
           exec_sql(qry) # status.
         end         
       end 
               
       # Result set not returned for execute_sql
       def execute_sql(qry)
         unless qry =~ my_select?
           exec_sql(qry)
         else
           raise ActiveSQLError, "no return of result set"
         end     
       end 
       alias :insert_sql :execute_sql       

       def establish_connection(config) 
         host     = (config[:host] ||= 'localhost')
         username = config[:username] ? config[:username].to_s : 'root'
         password = config[:password] ? config[:password].to_s : ''
         database = config[:database] 
         port     = config[:port] 
         socket   = config[:socket] 
         default_flags = 0 # No multi-results as of now.
         options = [host, username, password, database, port, socket, default_flags]
	 load_config(*options)
         # make connection across the parameters.
         connect!
      end 

     end 
  end 
end 
