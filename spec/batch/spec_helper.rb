require 'spec_helper'

DATABASE_NAME = 'rapids_test'
ActiveRecord::Base.establish_connection(
                      :adapter  => 'mysql',
                      :username => 'root',
                      :password => '',
                      :host     => 'localhost')
ActiveRecord::Base.connection.create_database(DATABASE_NAME) rescue ActiveRecord::StatementInvalid

ActiveRecord::Base.establish_connection(
                      :adapter  => 'mysql',
                      :database => DATABASE_NAME,
                      :username => 'root',
                      :password => '',
                      :host     => 'localhost')

class ColumnHelperMigrations < ActiveRecord::Migration
  def self.up
    create_table :posts do |t|
      t.column :name, :string, :null => false
      t.column :category, :string, :null => false
      t.column :author_id, :integer, :null => false
    end
    create_table :authors do |t|
      t.column :name, :string, :null => false
    end
    create_table :categories do |t|
      t.column :category, :string, :null => false
    end
    create_table :tags do |t|
      t.column :name, :string, :null => false
    end
    create_table :post_tags do |t|
      t.column :post_id, :integer, :null => false
      t.column :tag_id, :integer, :null => false
    end
    create_table :comments do |t|
      t.column :title, :string, :null => false
      t.column :body, :string, :null => false
    end
  end

  def self.down
    drop_table :posts
    drop_table :categories
    drop_table :authors
    drop_table :tags
    drop_table :post_tags
    drop_table :comments
  end
end

class Post < ActiveRecord::Base
  belongs_to :author
  belongs_to :blog
  has_many :post_tags
end

class Category < ActiveRecord::Base
end

class Author < ActiveRecord::Base
end

class Tag < ActiveRecord::Base
  has_many :post_tags
end

class PostTag < ActiveRecord::Base
  belongs_to :tag
  belongs_to :post
end

class Comment < ActiveRecord::Base
  belongs_to :post
end

RSpec.configure do |config|
  config.before(:suite) do
    ColumnHelperMigrations.migrate(:up)
  end
  config.after(:suite) do
    ColumnHelperMigrations.migrate(:down)
  end
end

def clean_sql(sql)
  sql.gsub(/(\s)\s*/,'\1').lstrip.rstrip
end