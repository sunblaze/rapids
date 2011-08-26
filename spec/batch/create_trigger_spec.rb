require 'batch/spec_helper'

module Rapids::Batch
  describe CreateTrigger do
    it "should generate sql for a simple batch definition" do
      batch = Rapids::Batch::DefineBatch.new
      
      create_trigger = CreateTrigger.new(Category,batch)
      clean_sql(create_trigger.to_sql).should == "create trigger `$categories_batch_trigger` after insert on `$categories_batch` for each row\nbegin\ninsert into `categories` (`category`)\nvalues (new.`category`);\nend"
    end
    
    it "should generate sql for a basic definition" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create(:author,[:name])
      
      create_trigger = CreateTrigger.new(Post,batch)
      clean_sql(create_trigger.to_sql).should == "create trigger `$posts_batch_trigger` after insert on `$posts_batch` for each row\nbegin\ndeclare find_or_create$author integer;\nselect id from `authors` where `name` <=> new.`foc$author$name` into find_or_create$author;\nif find_or_create$author is null then\ninsert into authors (`name`)\nvalues (new.`foc$author$name`);\nselect last_insert_id() into find_or_create$author;\nend if;\ninsert into `posts` (`category`,`name`,`author_id`)\nvalues (new.`category`,new.`name`,find_or_create$author);\nend"
    end
    
    it "should generate sql for a manual definition" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create("Category",[:category])
      
      create_trigger = CreateTrigger.new(Post,batch)
      clean_sql(create_trigger.to_sql).should == "create trigger `$posts_batch_trigger` after insert on `$posts_batch` for each row\nbegin\ndeclare find_or_create$Category integer;\nselect id from `categories` where `category` <=> new.`foc$Category$category` into find_or_create$Category;\nif find_or_create$Category is null then\ninsert into categories (`category`)\nvalues (new.`foc$Category$category`);\nend if;\ninsert into `posts` (`author_id`,`category`,`name`)\nvalues (new.`author_id`,new.`category`,new.`name`);\nend"
    end
    
    it "should generate sql for a manual definition, with explicit source column" do
      batch = Rapids::Batch::DefineBatch.new
      batch.find_or_create("AltCategory",[[:name,:category]])
      
      create_trigger = CreateTrigger.new(Post,batch)
      clean_sql(create_trigger.to_sql).should == "create trigger `$posts_batch_trigger` after insert on `$posts_batch` for each row\nbegin\ndeclare find_or_create$AltCategory integer;\nselect id from `alt_categories` where `name` <=> new.`foc$AltCategory$name` into find_or_create$AltCategory;\nif find_or_create$AltCategory is null then\ninsert into alt_categories (`name`)\nvalues (new.`foc$AltCategory$name`);\nend if;\ninsert into `posts` (`author_id`,`category`,`name`)\nvalues (new.`author_id`,new.`category`,new.`name`);\nend"
    end
    
    it "should generate sql for a replace insert instead" do
      batch = Rapids::Batch::DefineBatch.new
      
      create_trigger = CreateTrigger.new(Category,batch,:replace => true)
      clean_sql(create_trigger.to_sql).should == "create trigger `$categories_batch_trigger` after insert on `$categories_batch` for each row\nbegin\nreplace into `categories` (`category`)\nvalues (new.`category`);\nend"
    end
  end
end
