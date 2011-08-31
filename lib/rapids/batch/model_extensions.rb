module Rapids
  module Batch
    module ModelExtensions
      def columns(model = @model)
        model.columns.reject{|c|c.primary}
      end
      
      def batch_table_name
        "$#{@model.table_name}_batch"
      end
      
      def sql_column_name(column,hash_path)
        prefix = if hash_path.empty?
          ""
        elsif path_type = hash_path.first and path_type.is_a?(Hash) and path_type[:type] == :update
          "update$"
        else
          "foc$"
        end
        association_list = (hash_path + [column.name]).map do |path_type|
          if path_type.is_a?(Hash)
            path_type[:name]
          else
            path_type.to_s
          end
        end.join("$")
        "`#{prefix+association_list}`"
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
      
      #Stolen from ActiveRecord::Base
      def quote_bound_value(value)
        c = ActiveRecord::Base.connection
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
    end
  end
end