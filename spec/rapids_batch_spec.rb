require 'spec_helper'

class RapidsBatchTestCase < ActiveRecord::Base
  extend Rapids::Batch
  
  belongs_to :a_belongs_to
  
  batch do
    update :a_belongs_to
  end
end

describe Rapids::Batch do
end
