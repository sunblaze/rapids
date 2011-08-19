require 'rapids/batch/create_table'
require 'rapids/batch/insert_into'
require 'rapids/batch/create_trigger'
require 'rapids/batch/find_or_create'

module Rapids
  module Batch
    class DefineBatch
      attr_reader :find_or_creates
      
      def initialize
        @find_or_creates = []
      end
      
      def find_or_create(name,find_columns,*fill_columns_plus_other_params)
        @find_or_creates << FindOrCreate.new(name,find_columns,fill_columns_plus_other_params.first)
      end
    end
    
    def batch_create(collection)
      
      #TODO add support for non-activerecord class
      drop_batch_table_if_exists
      
      create_batch_table
      
      create_batch_trigger
      
      batch_insert(collection)
    end
    
    def batch(&block)
      @batch = DefineBatch.new
      @batch.instance_exec(&block) if block_given?
    end
    
    private
    def drop_batch_table_if_exists
      drop_table_sql = "DROP TABLE IF EXISTS `#{batch_table_name}`"
      self.connection.execute(drop_table_sql)
    end
    
    def create_batch_table
      create_table = CreateTable.new(self,@batch)
      
      connection.execute(create_table.to_sql)
    end
    
    def create_batch_trigger
      create_trigger = CreateTrigger.new(self,@batch)
      
      connection.execute(create_trigger.to_sql)
    end
    
    def batch_insert(values)
      insert_into = InsertInto.new(self,@batch,values)

      connection.execute(insert_into.to_sql)
    end
    
    #TODO refactor this it duplicates what's done in model_extensions.rb
    def batch_table_name
      "$#{table_name}_batch"
    end
  end
end