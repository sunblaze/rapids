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
      
      def generate_columns_hash(model,find_or_creates)
        hash = {}
        
        batch_association_primary_keys = if find_or_creates.is_a?(Hash)
          find_or_creates.map{|name,ignore|model.reflections[name].primary_key_name}
        else
          []
        end

        columns(model).reject{|c|batch_association_primary_keys.include?(c.name)}.each do |column|
          hash[column.name.to_sym] = column
        end
        
        if find_or_creates.is_a?(Hash)
          find_or_creates.each do |name,criteria|
            hash[name] = generate_columns_hash(model.reflections[name].klass,criteria)
          end
        end

        hash
      end
    end
  end
end