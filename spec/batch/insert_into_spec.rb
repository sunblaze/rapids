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
      clean_sql(insert_into.to_sql).should == "INSERT INTO `$posts_batch` (`foc$Category$category`,`author_id`,`category`,`name`) VALUES ('food',NULL,'food','Dining at 323 Butter St')"
    end
    
    it "should generate sql for a manual definition, with explicit source column" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create("AltCategory",[[:name,:category]])
      collection = [Post.new(:name => "Dining at 323 Butter St",:category => "food")]
      
      insert_into = InsertInto.new(Post,batch,collection)
      clean_sql(insert_into.to_sql).should == "INSERT INTO `$posts_batch` (`foc$AltCategory$name`,`author_id`,`category`,`name`) VALUES ('food',NULL,'food','Dining at 323 Butter St')"
    end
    
    it "should generate sql and ignore the replace option" do
      batch = Rapids::Batch::DefineBatch.new
      collection = %w{food politics}.map do |category_name|
        Category.new(:category => category_name)
      end
      
      insert_into = InsertInto.new(Category,batch,collection,:replace => true)
      clean_sql(insert_into.to_sql).should == "INSERT INTO `$categories_batch` (`category`) VALUES ('food'),('politics')"
    end
    
    it "should generate sql for an update definition" do
      batch = Rapids::Batch::DefineBatch.new
      batch.update(:author)
      collection = [Post.new(:name => "Dining at 323 Butter St",:category => "food",:author_id => 1,:author => Author.new(:name => "Joe"))]
      
      insert_into = InsertInto.new(Post,batch,collection)
      clean_sql(insert_into.to_sql).should == "INSERT INTO `$posts_batch` (`author_id`,`category`,`name`,`update$author$name`) VALUES (1,'food','Dining at 323 Butter St','Joe')"
    end
    
    describe "Without model objects" do
      it "should generate sql for a simple batch definition" do
        batch = Rapids::Batch::DefineBatch.new
        collection = %w{food politics}.map do |category_name|
          {"category" => category_name}
        end

        insert_into = InsertInto.new(Category,batch,collection)
        clean_sql(insert_into.to_sql).should == "INSERT INTO `$categories_batch` (`category`) VALUES ('food'),('politics')"
      end
      
      it "should generate sql for a basic definition" do
        batch = Rapids::Batch::DefineBatch.new
        batch.find_or_create(:author,[:name])
        collection = [{"name" => "Dining at 323 Butter St","category" => "food","author" => {"name" => "Joe"}}]

        insert_into = InsertInto.new(Post,batch,collection)
        clean_sql(insert_into.to_sql).should == "INSERT INTO `$posts_batch` (`foc$author$name`,`category`,`name`) VALUES ('Joe','food','Dining at 323 Butter St')"
      end

      it "should generate sql for a manual definition" do
        batch = Rapids::Batch::DefineBatch.new
        batch.find_or_create("Category",[:name])
        collection = [{"name" => "Dining at 323 Butter St","category" => "food"}]

        insert_into = InsertInto.new(Post,batch,collection)
        clean_sql(insert_into.to_sql).should == "INSERT INTO `$posts_batch` (`foc$Category$category`,`author_id`,`category`,`name`) VALUES ('food',NULL,'food','Dining at 323 Butter St')"
      end

      it "should generate sql for a manual definition, with explicit source column" do
        batch = Rapids::Batch::DefineBatch.new
        batch.find_or_create("AltCategory",[[:name,:category]])
        collection = [{"name" => "Dining at 323 Butter St","category" => "food"}]

        insert_into = InsertInto.new(Post,batch,collection)
        clean_sql(insert_into.to_sql).should == "INSERT INTO `$posts_batch` (`foc$AltCategory$name`,`author_id`,`category`,`name`) VALUES ('food',NULL,'food','Dining at 323 Butter St')"
      end
      
      it "should generate sql for a more complicated find or create with reliances on a has_many relationship" do
        batch = Rapids::Batch::DefineBatch.new
        batch.find_or_create(:post,[:name,{:post_tags => [:tag_id]}])
        collection = [{"title" => "You suck","body" => "Im a troll","post" => {"name" => "I just did something cool","post_tags" => [{"tag_id" => 1},{"tag_id" => 2}]}}]

        create_table = InsertInto.new(Comment,batch,collection)
        create_table.to_sql.should == "INSERT INTO `$comments_batch` (`body`,`foc$post$author_id`,`foc$post$category`,`foc$post$name`,`foc$post$post_tags$tag_id`,`title`) VALUES ('Im a troll',NULL,NULL,'I just did something cool','1,2','You suck')"
      end
      
      it "should generate sql for an update definition" do
        batch = Rapids::Batch::DefineBatch.new
        batch.update(:author)
        collection = [{"name" => "Dining at 323 Butter St","category" => "food","author_id" => 1,"author" => {"name" => "Joe"}}]

        insert_into = InsertInto.new(Post,batch,collection)
        clean_sql(insert_into.to_sql).should == "INSERT INTO `$posts_batch` (`author_id`,`category`,`name`,`update$author$name`) VALUES (1,'food','Dining at 323 Butter St','Joe')"
      end
    end
  end
end