require 'rapids/batch/model_extensions'
require 'rapids/batch/columns_helper'

module Rapids
  module Batch
    class InsertInto
      include ModelExtensions
      
      def initialize(model,batch_definition,values,options = {})
        @model = model
        @batch = batch_definition
        @values = values
        @options = options
      end
      
      def to_sql
        columns_helper = ColumnsHelper.new(@model,@batch)
        insert_header_sql = columns_helper.map{|column,path|sql_column_name(column,path)}.join(",")
        
        values_sql = @values.map do |row|
          row_sql = columns_helper.map do |column,path|
            source_column_name = column.name
            destination_column = column
            
            @batch.find_or_creates.each do |find_or_create|
              if find_or_create.name == path.first && find_or_create.find_columns.any?{|fc|fc.is_a?(Array) && fc.first.to_s == column.name}
                source_column_name = find_or_create.find_columns.detect{|fc|fc.is_a?(Array) && fc.first.to_s == column.name}.last.to_s
              end
            end
          
            specific_object = path.inject(row) do |memo,hash_or_association_name|
              association_name = if hash_or_association_name.is_a?(Hash)
                hash_or_association_name[:name]
              else
                hash_or_association_name
              end
              if memo.respond_to?(association_name)
                memo.send(association_name)
              elsif memo.is_a?(Hash) && memo[association_name.to_s]
                memo[association_name.to_s]
              elsif association_name.is_a?(String)
                memo
              end
            end

            if specific_object.is_a?(Array)
              many_attributes = specific_object.map do |s|
                if s.is_a?(ActiveRecord::Base)
                  s.instance_variable_get(:@attributes)[source_column_name]
                else
                  s[source_column_name]
                end
              end.compact
              if many_attributes.empty?
                default_on_nil(nil,destination_column)
              else
                default_on_nil(many_attributes.sort.join(","),destination_column)
              end
            else
              val = if specific_object.is_a?(ActiveRecord::Base)
                specific_object.instance_variable_get(:@attributes)[source_column_name] #speeds things up a bit since it's not typed first before being quoted
              else
                specific_object[source_column_name]
              end
              default_on_nil(val,destination_column)
            end
          end
          "(#{row_sql.join(",")})".encode("utf-8", :undef => :replace, :replace => "")
        end.join(",")
        
        "INSERT INTO `#{batch_table_name}` (#{insert_header_sql}) VALUES #{values_sql}"
      end
    end
  end
end