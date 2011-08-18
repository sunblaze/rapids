module Rapids
  module Batch
    class ColumnsHelper
      include Enumerable
      include ModelExtensions
      
      def initialize(model,find_or_creates)
        @hash = generate_columns_hash(model,find_or_creates)
      end
      
      def each(&block)
        internal_each(@hash,[],&block)
      end
      
      private
      def internal_each(columns_hash,hash_path,&block)
        columns_hash.each do |key,column_or_hash|
          if column_or_hash.is_a?(Hash)
            internal_each(column_or_hash,hash_path + [key],&block)
          else
            yield(column_or_hash,hash_path)
          end
        end
      end
      
      def generate_columns_hash(model,criteria_hash,skip_columns = [])
        hash = {}
        skip_columns_next_time = {}
        
        batch_association_primary_keys = if criteria_hash.is_a?(Hash)
          criteria_hash.map do |name,ignore|
            if model.reflections[name].collection?
              skip_columns_next_time[name] = [model.reflections[name].primary_key_name]
              nil
            else
              model.reflections[name].primary_key_name
            end
          end.compact
        else
          []
        end
        
        columns(model).reject{|c|batch_association_primary_keys.include?(c.name) || skip_columns.include?(c.name)}.each do |column|
          hash[column.name.to_sym] = column
        end
        
        if criteria_hash.is_a?(Hash)
          criteria_hash.each do |name,criteria_array|
            if model.reflections[name]
              if criteria_array.is_a?(Array)
                hash[name] = criteria_array.inject({}) do |memo,criteria|
                  memo.merge(generate_columns_hash(model.reflections[name].klass,criteria,skip_columns_next_time[name] || []))
                end
              end
            end
          end
        end
        
        hash        
      end
    end
  end
end