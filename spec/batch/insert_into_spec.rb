require 'batch/spec_helper'

module Rapids::Batch
  describe InsertInto do
    it "should generate sql for a simple batch definition" do
      batch = Rapids::Batch::DefineBatch.new
      collection = %w{food politics}.map do |category_name|
        Category.new(:category => category_name)
      end
      
      insert_into = InsertInto.new(Category,batch,collection)
      clean_sql(insert_into.to_sql).should == "INSERT INTO `$categories_batch` (`category`) VALUES ('food'),('politics')"
    end
    
    it "should generate sql for a basic definition" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create(:author,[:name])
      collection = [Post.new(:name => "Dining at 323 Butter St",:category => "food",:author => Author.new(:name => "Joe"))]
      
      insert_into = InsertInto.new(Post,batch,collection)
      clean_sql(insert_into.to_sql).should == "INSERT INTO `$posts_batch` (`foc$author$name`,`category`,`name`) VALUES ('Joe','food','Dining at 323 Butter St')"
    end
    
    it "should generate sql for a manual definition" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create("Category",[:name])
      collection = [Post.new(:name => "Dining at 323 Butter St",:category => "food")]
      
      insert_into = InsertInto.new(Post,batch,collection)
      clean_sql(insert_into.to_sql).should == ""
    end
  end
end