require 'rapids/batch/model_extensions'
require 'rapids/batch/columns_helper'

module Rapids
  module Batch
    class CreateTrigger
      include ModelExtensions
      
      def initialize(model,batch_definition)
        @model = model
        @batch = batch_definition
      end

      def to_sql
        declares = @batch.find_or_creates.map do |name,ignore|
          "declare find_or_create_#{name} integer;"
        end.join("\n")

        columns_helper = ColumnsHelper.new(@model,@batch.find_or_creates)
        main_columns = columns_helper.find_all{|column,path| path == []}

        insert_header = (main_columns.map(&:first) + column_names(@model,@batch.find_or_creates.keys)).map{|a|sql_column_name(a,[])}
        insert_values = main_columns.map{|a|"new.#{sql_column_name(a.first,[])}"} + @batch.find_or_creates.keys.map{|name|"find_or_create_#{name}"}

        <<-TRIGGER_SQL
          create trigger `#{batch_table_name}_trigger` after insert on `#{batch_table_name}` for each row
          begin
            #{declares}
            
            #{find_or_create_sql(@model,@batch.find_or_creates)}
            
            insert into `#{@model.table_name}` (#{insert_header.join(",")})
                                        values (#{insert_values.join(",")});
          end
        TRIGGER_SQL
      end
      
      private
      def find_or_create_sql(model,find_or_creates)
        columns_helper = ColumnsHelper.new(@model,@batch.find_or_creates)
        
        find_or_creates.map do |name,criteria|
          sub_model = model.reflections[name].klass
          association_table_name = sub_model.table_name
          sub_model_columns = columns_helper.find_all{|column,path| path == [name]}
          
          where_sql = columns_helper.find_all{|column,path| path == [name] && column_names(sub_model,criteria).include?(column)}.map{|c|"#{sql_column_name(c.first,[])} <=> new.#{sql_column_name(c.first,c.last)}"}.join(" and ")
          
          "select id from `#{association_table_name}` where #{where_sql} into find_or_create_#{name};
           if find_or_create_#{name} is null then
             insert into #{association_table_name} (#{sub_model_columns.map{|a|sql_column_name(a.first,[])}.join(",")})
                                            values (#{sub_model_columns.map{|a|"new.#{sql_column_name(a.first,a.last)}"}.join(",")});
             
             select last_insert_id() into find_or_create_#{name};
           end if;
          "
        end.join("\n")
      end
      
      def column_names(model,criteria_array)
        criteria_array.map do |column_or_association_name|
          column = model.columns.detect{|c|c.name == column_or_association_name.to_s}

          unless column
            # doesn't match a column name look for an assocation instead then
            association = model.reflections[column_or_association_name]

            column = if association.collection?
              nil #TODO implement
            else
              model.columns.detect{|c|c.name == association.primary_key_name}
            end
          end

          column
        end
      end
    end
  end
end
