require 'rapids/batch/model_extensions'
require 'rapids/batch/columns_helper'

module Rapids
  module Batch
    class CreateTable
      include ModelExtensions
      
      def initialize(model,batch_definition)
        @model = model
        @batch = batch_definition
      end
      
      def to_sql
        columns_helper = ColumnsHelper.new(@model,@batch.find_or_creates)
        columns_sql = columns_helper.map{|column,path|"#{sql_column_name(column,path)} #{column.sql_type}"}.join(",")

        "CREATE TABLE `#{batch_table_name}` (#{columns_sql}) ENGINE=BLACKHOLE DEFAULT CHARSET=utf8"
      end
    end
  end
end