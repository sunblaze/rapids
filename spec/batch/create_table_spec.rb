require 'batch/spec_helper'

module Rapids::Batch
  describe CreateTable do
    it "should generate sql for a simple batch definition" do
      batch = Rapids::Batch::DefineBatch.new
      
      create_table = CreateTable.new(Category,batch)
      create_table.to_sql.should == "CREATE TABLE `$categories_batch` (`name` varchar(255)) ENGINE=BLACKHOLE DEFAULT CHARSET=utf8"
    end
    
    it "should generate sql for a basic definition" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create(:author,[:name])
      
      create_table = CreateTable.new(Post,batch)
      create_table.to_sql.should == "CREATE TABLE `$posts_batch` (`foc$author$name` varchar(255),`name` varchar(255),`category` varchar(255)) ENGINE=BLACKHOLE DEFAULT CHARSET=utf8"
    end
    
    it "should generate sql for a manual definition" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create("Category",[:name])
      
      create_table = CreateTable.new(Post,batch)
      create_table.to_sql.should == "CREATE TABLE `$posts_batch` (`author_id` int(11),`name` varchar(255),`foc$Category$name` varchar(255),`category` varchar(255)) ENGINE=BLACKHOLE DEFAULT CHARSET=utf8"
    end
    
    it "should generate sql for a more complicated find or create with reliances on a has_many relationship" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create(:post,[:name,{:post_tags => [:tag_id]}])
      
      create_table = CreateTable.new(Comment,batch)
      create_table.to_sql.should == "CREATE TABLE `$comments_batch` (`body` varchar(255),`foc$post$author_id` int(11),`foc$post$name` varchar(255),`foc$post$post_tags$tag_id` varchar(255),`foc$post$category` varchar(255),`title` varchar(255)) ENGINE=BLACKHOLE DEFAULT CHARSET=utf8"
    end
  end
end
