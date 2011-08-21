require 'batch/spec_helper'

module Rapids::Batch
  describe ColumnsHelper do
    it "should intialize with an empty find or create list" do
      ColumnsHelper.new(Post,[])
    end

    it "should initialize with the association find or create" do
      find_or_creates = [FindOrCreate.new(:author,[:name],[])]
      columns_helper = ColumnsHelper.new(Post,find_or_creates)
      
      columns_helper.map{|a|a.first.name}.include?("name").should be_true
      columns_helper.map{|a|a.first.name}.include?("author_id").should be_false, "expected author_id not to be present in this list:\n#{columns_helper.to_a.inspect}"
    end
    
    it "should initialize with the manual find or create format" do
      find_or_creates = [FindOrCreate.new("Category",[:category],[])]
      columns_helper = ColumnsHelper.new(Post,find_or_creates)
      
      columns_helper.any? do |pair|
        column,path = *pair
        column.name == "category" && path == ["Category"]
      end.should be_true, "expected to find a column named 'category' and the path to be [\"Category\"] in the following:\n#{columns_helper.to_a.inspect}"
    end
    
    it "should initialize with the association find or create with a has_many find or create" do
      find_or_creates = [FindOrCreate.new(:post,[:name,{:post_tags => [:tag_id]}],[])]
      columns_helper = ColumnsHelper.new(Comment,find_or_creates)
      
      columns_helper.any? do |pair|
        column,path = *pair
        column.name == "tag_id" && path == [:post,:post_tags]
      end.should be_true, "expected to find a column named 'tag_id' and the path to be [:post,:post_tags] in the following:\n#{columns_helper.to_a.inspect}"
    end
  end
end
