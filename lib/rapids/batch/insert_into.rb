require 'rapids/batch/model_extensions'
require 'rapids/batch/columns_helper'

module Rapids
  module Batch
    class InsertInto
      include ModelExtensions
      
      def initialize(model,batch_definition,values)
        @model = model
        @batch = batch_definition
        @values = values
      end
      
      def to_sql
        columns_helper = ColumnsHelper.new(@model,@batch.find_or_creates)
        insert_header_sql = columns_helper.map{|column,path|sql_column_name(column,path)}.join(",")
        values_sql = @values.map do |row|
          row_sql = columns_helper.map do |column,path|
            specific_object = path.inject(row){|memo,association|memo.send(association)}
            if specific_object.respond_to?(:each)
              many_attributes = specific_object.map{|s|s.attributes[column.name]}.compact
              if many_attributes.empty?
                default_on_nil(nil,column)
              else
                default_on_nil(many_attributes.sort.join(","),column)
              end
            else
              default_on_nil(specific_object.attributes[column.name],column)
            end
          end
          "(#{row_sql.join(",")})"
        end.join(",")
        
        "INSERT INTO `#{batch_table_name}` (#{insert_header_sql}) VALUES #{values_sql}"
      end
    end
  end
end