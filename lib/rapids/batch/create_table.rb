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
        columns_helper = ColumnsHelper.new(@model,@batch)
        columns_sql = columns_helper.map do |column,path|
          association = nil
          model = @model
          path.each do |association_name_or_hash|
            association_name = if association_name_or_hash.is_a?(Hash)
              association_name_or_hash[:name]
            else
              association_name_or_hash
            end
            if model.reflections[association_name]
              association = model.reflections[association_name]
              model = model.reflections[association_name].klass
            end
          end
          column_type = if association && association.collection? && column.number?
            "varchar(255)"
          else
            column.sql_type
          end
          "#{sql_column_name(column,path)} #{column_type}"
        end.join(",")

        "CREATE TABLE `#{batch_table_name}` (#{columns_sql}) ENGINE=BLACKHOLE DEFAULT CHARSET=utf8 COLLATE=#{@model.connection.collation}"
      end
    end
  end
end