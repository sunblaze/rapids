require 'rapids'

class RapidsBatchTestCase
  extend Rapids::Batch
end

describe Rapids::Batch do
  it "should provide batch_create to a class when included" do
    RapidsBatchTestCase.batch_create([])
  end
end
