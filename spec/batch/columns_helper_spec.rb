require 'batch/spec_helper'

module Rapids::Batch
  describe ColumnsHelper do
    it "should intialize with an empty find or create list" do
      batch = Rapids::Batch::DefineBatch.new
      ColumnsHelper.new(Post,batch)
    end

    it "should initialize with the association find or create" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create(:author,[:name])
      columns_helper = ColumnsHelper.new(Post,batch)
      
      columns_helper.map{|a|a.first.name}.include?("name").should be_true
      columns_helper.map{|a|a.first.name}.include?("author_id").should be_false, "expected author_id not to be present in this list:\n#{columns_helper.to_a.inspect}"
    end
    
    it "should initialize with the manual find or create format" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create("Category",[:category])
      columns_helper = ColumnsHelper.new(Post,batch)
      
      columns_helper.any? do |pair|
        column,path = *pair
        column.name == "category" && path == ["Category"]
      end.should be_true, "expected to find a column named 'category' and the path to be [\"Category\"] in the following:\n#{columns_helper.to_a.inspect}"
    end
    
    it "should initialize with the association find or create with a has_many find or create" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create(:post,[:name,{:post_tags => [:tag_id]}])
      columns_helper = ColumnsHelper.new(Comment,batch)
      
      columns_helper.any? do |pair|
        column,path = *pair
        column.name == "tag_id" && path == [:post,:post_tags]
      end.should be_true, "expected to find a column named 'tag_id' and the path to be [:post,:post_tags] in the following:\n#{columns_helper.to_a.inspect}"
    end
    
    it "should initialize with the update definition and have the appropriate columns" do
      batch = Rapids::Batch::DefineBatch.new
      batch.update(:author)
      columns_helper = ColumnsHelper.new(Post,batch)
      
      columns_helper.any? do |pair|
        column,path = *pair
        column.name == "name" && path == [{:type => :update,:name => :author}]
      end.should be_true, "expected to find a column named 'name' and the path to be [{:type => :update,:name => :author}] in the following:\n#{columns_helper.to_a.inspect}"
    end
  end
end
