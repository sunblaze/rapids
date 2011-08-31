require 'rapids/batch/create_table'
require 'rapids/batch/insert_into'
require 'rapids/batch/create_trigger'
require 'rapids/batch/find_or_create'
require 'rapids/batch/update'

module Rapids
  module Batch
    class DefineBatch
      attr_reader :find_or_creates, :updates
      
      def initialize
        @find_or_creates = []
        @updates = []
      end
      
      def find_or_create(name,find_columns,*fill_columns_plus_other_params)
        @find_or_creates << FindOrCreate.new(name,find_columns,fill_columns_plus_other_params.first)
      end
      
      def update(name)
        @updates << Update.new(name)
      end
    end
    
    def batch_create(collection,options = {})
      drop_batch_table_if_exists
      
      create_batch_table
      
      create_batch_trigger(options)
      
      batch_insert(collection,options)
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
    
    def create_batch_trigger(options)
      create_trigger = CreateTrigger.new(self,@batch,options)

      connection.execute(create_trigger.to_sql)
    end
    
    def batch_insert(values,options)
      insert_into = InsertInto.new(self,@batch,values,options)

      connection.execute(insert_into.to_sql)
    end
    
    #TODO refactor this it duplicates what's done in model_extensions.rb
    def batch_table_name
      "$#{table_name}_batch"
    end
  end
end