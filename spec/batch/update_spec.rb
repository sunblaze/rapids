require 'batch/spec_helper'

module Rapids::Batch
  describe Update do
    it "should initialize with a name and the name should be retrievable" do
      u = Update.new(:hello)
      u.name.should == :hello
    end
    
    it "should give me it's path hash" do
      u = Update.new(:hello)
      u.path.should == [{:type => :update, :name => :hello}]
    end
  end
end
