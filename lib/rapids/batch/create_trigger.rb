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
          "declare #{variable_name(name,[])} integer;"
        end.join("\n")

        columns_helper = ColumnsHelper.new(@model,@batch.find_or_creates)
        main_columns = columns_helper.find_all{|column,path| path == []}

        insert_header = (main_columns.map(&:first) + criteria_columns(@model,@batch.find_or_creates.keys)).map{|a|sql_column_name(a,[])}
        insert_values = main_columns.map{|a|"new.#{sql_column_name(a.first,[])}"} + @batch.find_or_creates.keys.map{|name|variable_name(name,[])}

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
      def find_or_create_sql(model,find_or_creates, recursion_path = [])
        columns_helper = ColumnsHelper.new(model,find_or_creates)
        
        find_or_creates.map do |name,criteria|
          sub_model = model.reflections[name].klass
          association_table_name = sub_model.table_name
          sub_model_columns = columns_helper.find_all{|column,path| path == [name]}
          
          where_sql = foc_where_sql(sub_model,criteria,[name])

          "select id from `#{association_table_name}` where #{where_sql} into #{variable_name(name,recursion_path)};
           if #{variable_name(name,recursion_path)} is null then
             insert into #{association_table_name} (#{sub_model_columns.map{|a|sql_column_name(a.first,[])}.join(",")})
                                            values (#{sub_model_columns.map{|a|"new.#{sql_column_name(a.first,a.last)}"}.join(",")});
             
             select last_insert_id() into #{variable_name(name,recursion_path)};
             
             #{sub_inserts(sub_model,criteria,[name],variable_name(name,recursion_path))}
           end if;
          "
        end.join("\n")
      end
      
      def variable_name(name,recursion_path)
        "find_or_create$#{(recursion_path + [name]).join("$")}"
      end
      
      def foc_where_sql(model,criteria_array,path)
        and_comparisons = criteria_array.map do |column_or_association_name_or_hash|
          if column_or_association_name_or_hash.is_a?(Hash)
            sub_comparisons = column_or_association_name_or_hash.map do |association_name,sub_criteria|
              association = model.reflections[association_name]
              if association.collection?
                sub_criteria.map do |column_name|
                  column = lookup_column_by_name(association.klass,column_name)
                  "new.#{sql_column_name(column,path+[association_name])} = (select GROUP_CONCAT(#{sql_column_name(column,[])} ORDER BY #{sql_column_name(column,[])})
                        from #{association.quoted_table_name}
                        where `#{model.table_name}`.id = `#{association.primary_key_name}`)
                  "
                end.join(" and ")
              else
                "true"
              end
            end
            
            "("+sub_comparisons.join(" and ")+")"
          else
            column = lookup_column_by_name(model,column_or_association_name_or_hash)

            "#{sql_column_name(column,[])} <=> new.#{sql_column_name(column,path)}"
          end
        end
        
        and_comparisons.join(" and ")
      end
      
      def sub_inserts(model,criteria_array,path,key_variable)
        criteria_array.map do |column_or_association_name_or_hash|
          if column_or_association_name_or_hash.is_a?(Hash)
            column_or_association_name_or_hash.map do |association_name,sub_criteria|
              association = model.reflections[association_name]
              column = lookup_column_by_name(association.klass,sub_criteria.first)
              "begin
                 DECLARE cur_position INT DEFAULT 1;
                 DECLARE remainder VARCHAR(255);
                 DECLARE cur_string VARCHAR(255);

                 SET remainder = new.#{sql_column_name(column,path+[association_name])};

                 WHILE CHAR_LENGTH(remainder) > 0 AND cur_position > 0 DO
                   SET cur_position = INSTR(remainder, ',');
                   IF cur_position = 0 THEN
                     SET cur_string = remainder;
                   ELSE
                     SET cur_string = LEFT(remainder, cur_position - 1);
                   END IF;
                   INSERT INTO #{association.quoted_table_name} (#{["`#{association.primary_key_name}`",sql_column_name(column,[])].join(",")}) VALUES (#{[key_variable,'cur_string'].join(",")});
                   SET remainder = SUBSTRING(remainder, cur_position + 1);
                 END WHILE;
               end;"
            end.join("\n")
          end
        end.join("\n")
      end
      
      def lookup_column_by_name(model,column_name)
        column = model.columns.detect{|c|c.name == column_name.to_s}

        unless column
          # doesn't match a column name look for an assocation instead then
          association = model.reflections[column_name]

          column = if association.collection?
            nil #TODO implement
          else
            model.columns.detect{|c|c.name == association.primary_key_name}
          end
        end
        column
      end
      
      def criteria_columns(model,criteria_array)
        criteria_array.map do |column_or_association_name_or_hash|
          unless column_or_association_name_or_hash.is_a?(Hash)
            lookup_column_by_name(model,column_or_association_name_or_hash)
          end
        end
      end
    end
  end
end
