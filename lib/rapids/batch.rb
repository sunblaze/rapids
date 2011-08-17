require 'rapids/batch/create_table'
require 'rapids/batch/insert_into'
require 'rapids/batch/create_trigger'

module Rapids
  module Batch
    class DefineBatch
      attr_reader :find_or_creates
      
      def initialize
        @find_or_creates = {}
      end
      
      def find_or_create(name,find_columns)
        @find_or_creates[name] = find_columns
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
    
    def insert_header(model = self,find_or_creates = @batch.find_or_creates)
      columns_to_insert(model,find_or_creates).map{|c|"`#{c.name}`"}.join(",")
    end
    
    def insert_values(model = self,find_or_creates = @batch.find_or_creates,parents = [])
      columns = columns_to_insert(model,find_or_creates)
      prefix = parents.empty? ? "" : "foc_"
      parent_chain = parents.join("$")
      parent_chain += "__" unless parent_chain.empty?
      
      columns.map do |column|
        "new.`#{prefix}#{parent_chain}#{column.name}`"
      end.join(",")
    end
    
    def columns_to_insert(model = self,find_or_creates = @batch.find_or_creates)
      batch_association_primary_keys = if find_or_creates.is_a?(Hash)
        find_or_creates.map{|name,ignore|reflections[name].primary_key_name}
      else
        []
      end
      
      model.columns.reject{|c|c.primary}.reject{|c|batch_association_primary_keys.include?(c.name)}
    end
    
    def batch_table_name
      "$#{table_name}_batch"
    end
    
    def foc_column_name(parent,criteria)
      "`foc_#{parent}__#{criteria}`"
    end
    
    #TODO I want to refactor this method and the two below it
    def discover_column_through_association(name,column_or_association_name)
      column = reflections[name].klass.columns.detect{|c|c.name == column_or_association_name.to_s}
      
      unless column
        # doesn't match a column name look for an assocation instead then
        association = reflections[name].klass.reflections[column_or_association_name]
        
        column = if association.collection?
          nil #TODO implement
        else
          reflections[name].klass.columns.detect{|c|c.name == association.primary_key_name}
        end
      end
      
      column
    end
    
    def discover_column_name(name,column_or_association_name)
      discover_column_through_association(name,column_or_association_name).name
    end
    
    def discover_association_type(name,column_or_association_name)
      discover_column_through_association(name,column_or_association_name).sql_type
    end
    
    #Stolen from ActiveRecord::Base
    def quote_bound_value(value)
      c = self.connection
      if value.respond_to?(:map) && !value.acts_like?(:string)
        if value.respond_to?(:empty?) && value.empty?
          c.quote(nil)
        else
          value.map { |v| c.quote(v) }.join(',')
        end
      else
        c.quote(value)
      end
    end
    
    def default_on_nil(value,column)
      if value.nil?
        case
        when %w{created_at updated_at}.include?(column.name)
          "UTC_TIMESTAMP()"
        else
          quote_bound_value(value)
        end
      else
        quote_bound_value(value)
      end
    end
  end
end