module Rapids
  module Batch
    class FindOrCreate
      attr_reader :name, :find_columns, :fill_columns
      
      def initialize(name,find_columns,fill_columns)
        @name = name
        @find_columns = find_columns
        @fill_columns = fill_columns
      end
    end
  end
end