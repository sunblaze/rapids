module Rapids
  module Batch
    class Update
      attr_reader :name
      
      def initialize(name)
        @name = name
      end
      
      def path
        [{:type => :update, :name => name}]
      end
    end
  end
end
